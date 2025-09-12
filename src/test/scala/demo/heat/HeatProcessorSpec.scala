// src/test/scala/demo/heat/HeatProcessorSpec.scala
package demo.heat

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.streaming._ // for OutputMode, TimeMode
import java.time.Duration

final class HeatProcessorSpec extends AnyFunSuite with Matchers {

  private val testKey = "GAME|RIOT#ID|ORDER"

  private def newProc(): HeatProcessor =
    new HeatProcessor(
      TTLConfig(Duration.ofHours(2)),
      injectedStore = Some(new InMemoryHeatStateStore(HeatProcessor.InitialState))
    )

  // helpers to build HeatIn
  private def pulse(ts: Long, et: String, role: String, eid: Long) =
    HeatIn("GAME", "ORDER", "RIOT#ID", ts, "pulse", Some(et), None, Some(role),
      None, None, Some(eid), None, None, None, None)

  private def pulseNoEid(ts: Long, et: String, role: String) =
    HeatIn("GAME", "ORDER", "RIOT#ID", ts, "pulse", Some(et), None, Some(role),
      None, None, None, None, None, None, None)

  private def snapshot(ts: Long, level: Int, inv: Double, k: Int, d: Int, a: Int, cs: Double, champ: String) =
    HeatIn("GAME", "ORDER", "RIOT#ID", ts, "snapshot", None, Some(champ), None,
      Some(level), Some(inv), None, Some(k), Some(d), Some(a), Some(cs))

  // No timers used by our code; pass null safely.
  private val NoTimers: TimerValues = null.asInstanceOf[TimerValues]

  test("assist yields momentum; duplicate eid does not emit; next eid emits") {
    val p = newProc()

    val out1 = p.handleInputRows(testKey, Iterator(pulse(10_000L,"ChampionKill","assist",100L)), NoTimers).toList
    out1 should have length 1
    val m1 = out1.head.momentumRaw
    m1 should be > 0.0

    val outDup = p.handleInputRows(testKey, Iterator(pulse(12_000L,"ChampionKill","assist",100L)), NoTimers).toList
    outDup shouldBe empty // dedupe → decay-only → no emit

    val out2 = p.handleInputRows(testKey, Iterator(pulse(14_000L,"ChampionKill","assist",101L)), NoTimers).toList
    out2 should have length 1
    out2.head.momentumRaw should be > 0.0
  }

  test("spree increments on killer, resets on victim") {
    val p = newProc()

    val o1 = p.handleInputRows(testKey, Iterator(pulse(5_000L,"ChampionKill","killer",1L)), NoTimers).toList
    o1.head.spreeStreak shouldBe 1

    val o2 = p.handleInputRows(testKey, Iterator(pulse(8_000L,"ChampionKill","killer",2L)), NoTimers).toList
    o2.head.spreeStreak shouldBe 2

    val o3 = p.handleInputRows(testKey, Iterator(pulse(10_000L,"ChampionKill","victim",3L)), NoTimers).toList
    o3.head.spreeStreak shouldBe 0
  }

  test("snapshot persists decayed baseline; monotone merge; emits only on deltas") {
    val p = newProc()

    val outPulse = p.handleInputRows(testKey, Iterator(pulse(10_000L,"ChampionKill","killer",10L)), NoTimers).toList
    val m0 = outPulse.head.momentumRaw

    val outSnap1 = p.handleInputRows(testKey, Iterator(snapshot(55_000L,6,2000.0,2,1,3,80.0,"Wukong")), NoTimers).toList
    outSnap1 should have length 1
    outSnap1.head.momentumRaw should be < m0
    outSnap1.head.kills shouldBe 2
    outSnap1.head.championName shouldBe "Wukong"

    val outSnap2 = p.handleInputRows(testKey, Iterator(snapshot(56_000L,6,1500.0,2,1,3,70.0,"MonkeyKing")), NoTimers).toList
    outSnap2 shouldBe empty // lower cs/inv; name already set → no emit
  }

  test("death cannot drive momentum below zero and resets spree") {
    val p = newProc()

    // small momentum first
    p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "assist", 1L)), NoTimers).toList.head.momentumRaw should be > 0.0

    // death larger in magnitude than current momentum → clamp to 0
    val out = p.handleInputRows(testKey, Iterator(pulse(12_000L, "ChampionKill", "victim", 2L)), NoTimers).toList
    out should have length 1
    out.head.momentumRaw should be >= 0.0
    out.head.spreeStreak shouldBe 0
  }

  test("momentum caps: repeated big positives saturate momentumNorm to 100") {
    val p = newProc()

    // hammer with Barons to saturate quickly
    var eid = 1L
    var last = 0.0
    (0 until 10).foreach { i =>
      val o = p.handleInputRows(testKey, Iterator(pulse(10_000L + i, "BaronKill", "killer", {
        eid += 1; eid
      })), NoTimers).toList
      o should have length 1
      last = o.head.momentumNorm
    }
    last shouldBe 100.0 +- 0.0001
  }

  test("assist vs killer on DragonKill: assist adds less momentum than killer") {
    val p1 = newProc()
    val a = p1.handleInputRows(testKey, Iterator(pulse(10_000L, "DragonKill", "assist", 1L)), NoTimers).toList.head.momentumRaw

    val p2 = newProc()
    val k = p2.handleInputRows(testKey, Iterator(pulse(10_000L, "DragonKill", "killer", 1L)), NoTimers).toList.head.momentumRaw

    a should be < k
  }

  test("dedupe: non-increasing eventId is ignored even if later ts") {
    val p = newProc()

    val first = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "killer", 5L)), NoTimers).toList
    first should have length 1
    val m1 = first.head.momentumRaw

    val dupOlderId = p.handleInputRows(testKey, Iterator(pulse(12_000L, "ChampionKill", "killer", 4L)), NoTimers).toList
    dupOlderId shouldBe empty

    // momentum unchanged by deduped event
    val snap = p.handleInputRows(testKey, Iterator(snapshot(12_001L, 6, 0.0, 0, 0, 0, 0.0, "")), NoTimers).toList
    snap.head.momentumRaw shouldBe m1 +- 1e-9
  }

  test("out-of-order timestamp still accepted (dt=0) when eventId increases") {
    val p = newProc()

    val o1 = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "assist", 10L)), NoTimers).toList
    val mBefore = o1.head.momentumRaw

    // older ts but larger eventId → accepted, dt=0 so no decay
    val o2 = p.handleInputRows(testKey, Iterator(pulse(9_000L, "ChampionKill", "assist", 11L)), NoTimers).toList
    o2 should have length 1
    o2.head.momentumRaw should be > mBefore
  }

  test("snapshot with level-only increase changes power and emits") {
    val p = newProc()

    val s1 = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 1, 0.0, 0, 0, 0, 0.0, "")), NoTimers).toList
    val p1 = s1.head.powerNorm

    val s2 = p.handleInputRows(testKey, Iterator(snapshot(2_000L, 2, 0.0, 0, 0, 0, 0.0, "")), NoTimers).toList
    s2 should have length 1
    s2.head.powerNorm should be > p1
  }

  test("championName is sticky: empty→set emits; later different name does not override") {
    val p = newProc()

    p.handleInputRows(testKey, Iterator(snapshot(1_000L, 1, 0.0, 0, 0, 0, 0.0, "")), NoTimers).toList // empty name
    val o2 = p.handleInputRows(testKey, Iterator(snapshot(2_000L, 1, 0.0, 0, 0, 0, 0.0, "Syndra")), NoTimers).toList
    o2 should have length 1
    o2.head.championName shouldBe "Syndra"

    val o3 = p.handleInputRows(testKey, Iterator(snapshot(3_000L, 1, 0.0, 0, 0, 0, 0.0, "MonkeyKing")), NoTimers).toList
    o3 shouldBe empty // no override, no other deltas
  }

  test("powerFrom handles None level/inv gracefully (power stays 0)") {
    val p = newProc()

    val s = HeatIn("GAME", "ORDER", "RIOT#ID", 1_000L, "snapshot",
      etype = None, championName = None, role = None,
      level = None, invValue = None, eventId = None,
      kills = Some(0), deaths = Some(0), assists = Some(0), creepScore = Some(0.0))

    val out = p.handleInputRows(testKey, Iterator(s), NoTimers).toList
    out should have length 1
    out.head.powerNorm shouldBe 0.0 +- 1e-9
  }

  test("unknown pulse type emits decayed baseline without adding momentum") {
    val p = newProc()

    val o1 = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "assist", 1L)), NoTimers).toList
    val m0 = o1.head.momentumRaw

    // unknown type; ensures changedPulse=true, base=0 → only decay applies
    val out = p.handleInputRows(testKey, Iterator(pulse(55_000L, "WeirdThing", "killer", 2L)), NoTimers).toList
    out should have length 1
    out.head.momentumRaw should be < m0
  }

  test("pure decay tick via unknown kind does not emit") {
    val p = newProc()

    // seed state
    p.handleInputRows(testKey, Iterator(snapshot(1_000L, 3, 900.0, 0, 0, 0, 10.0, "")), NoTimers).toList

    val decayOnly = HeatIn("GAME", "ORDER", "RIOT#ID", 30_000L, "tick",
      etype = None, championName = None, role = None,
      level = None, invValue = None, eventId = None,
      kills = None, deaths = None, assists = None, creepScore = None)

    val out = p.handleInputRows(testKey, Iterator(decayOnly), NoTimers).toList
    out shouldBe empty
  }

  test("half-life ≈ halves momentum after 45s using a no-op pulse to anchor emit") {
    val p = newProc()

    val o1 = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "killer", 1L)), NoTimers).toList
    val m0 = o1.head.momentumRaw
    m0 should be > 0.0

    // after 45s, send an unknown-type pulse with new eid → emit decayed baseline
    val o2 = p.handleInputRows(testKey, Iterator(pulse(55_000L, "Noop", "assist", 2L)), NoTimers).toList
    val mHalf = o2.head.momentumRaw

    mHalf shouldBe (m0 / 2.0) +- 0.25 // allow slack: m0 depends on weights; decay model exact, but tolerate jitter
  }

  test("key routing: gameId/team/riotId are taken from the key") {
    val p = newProc()
    val key2 = "G1|Summ#EU|CHAOS"

    val out = p.handleInputRows(key2, Iterator(snapshot(1_000L, 3, 500.0, 0, 0, 0, 0.0, "Syndra")), NoTimers).toList
    out should have length 1
    out.head.gameId shouldBe "G1"
    out.head.riotId shouldBe "Summ#EU"
    out.head.team shouldBe "CHAOS"
  }

  test("pulses with missing eventId are not deduped: two emits and cumulative momentum") {
    val p = newProc()

    val a1 = p.handleInputRows(testKey, Iterator(pulseNoEid(10_000L, "ChampionKill", "assist")), NoTimers).toList
    val m1 = a1.head.momentumRaw

    val a2 = p.handleInputRows(testKey, Iterator(pulseNoEid(12_000L, "ChampionKill", "assist")), NoTimers).toList
    a2 should have length 1
    a2.head.momentumRaw should be > m1
  }

  test("micro-batch sorts by ts: reversed input in one batch equals ascending across two batches (momentumRaw)") {
    val pA = newProc()
    // One batch, reversed order → processor sorts
    val outA = pA.handleInputRows(testKey,
      Iterator(
        pulse(12_000L, "ChampionKill", "assist", 2L),
        pulse(10_000L, "ChampionKill", "assist", 1L)
      ),
      NoTimers
    ).toList
    outA should have length 1
    val mA = outA.head.momentumRaw

    val pB = newProc()
    val b1 = pB.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "assist", 1L)), NoTimers).toList
    val b2 = pB.handleInputRows(testKey, Iterator(pulse(12_000L, "ChampionKill", "assist", 2L)), NoTimers).toList
    val mB = b2.head.momentumRaw

    mA shouldBe mB +- 1e-9
  }

  test("normalization bounds: momentumNorm and heat are clamped to [0,100] and backendLatency >= 0") {
    val p = newProc()

    // try to push over the cap
    val o = p.handleInputRows(testKey, Iterator(
      pulse(10_000L, "BaronKill", "killer", 1L),
      pulse(10_100L, "BaronKill", "killer", 2L),
      pulse(10_200L, "BaronKill", "killer", 3L),
      pulse(10_300L, "BaronKill", "killer", 4L),
      pulse(10_400L, "BaronKill", "killer", 5L)
    ), NoTimers).toList

    o should have length 1
    o.head.momentumNorm should be >= 0.0
    o.head.momentumNorm should be <= 100.0
    o.head.heat should be >= 0.0
    o.head.heat should be <= 100.0
    o.head.backendLatencyMs should be >= 0L
    o.head.sourceTsMs should be > 0L
  }
}