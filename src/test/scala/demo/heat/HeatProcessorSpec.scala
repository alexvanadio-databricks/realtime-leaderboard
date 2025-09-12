package demo.heat

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.streaming._ // for OutputMode, TimeMode
import java.time.Duration
import HeatProcessor._

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

  test("dedupe: non-increasing eventId advances time & applies decay; next emit shows pure decay baseline") {
    val p = newProc()

    // Accepted event at t=10_000 → establishes m1
    val first = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "killer", 5L)), NoTimers).toList
    first should have length 1
    val m1 = first.head.momentumRaw
    m1 should be > 0.0

    // Deduped event at t=12_000 (eid=4 <= 5) → applies decay + advances lastTs, but does NOT emit
    val dupOlderId = p.handleInputRows(testKey, Iterator(pulse(12_000L, "ChampionKill", "killer", 4L)), NoTimers).toList
    dupOlderId shouldBe empty

    // Next emit (snapshot at t=12_001) should show m1 decayed over the FULL 2001 ms
    val outSnap = p.handleInputRows(testKey, Iterator(snapshot(12_001L, 6, 0.0, 0, 0, 0, 0.0, "")), NoTimers).toList
    outSnap should have length 1
    val expected = m1 * math.pow(0.5, (12_001L - 10_000L) / 45_000.0) // half-life = 45s
    outSnap.head.momentumRaw shouldBe expected +- 1e-9

    // And a new valid pulse (eid increases) should push momentum above the pure-decay baseline
    val outNext = p.handleInputRows(testKey, Iterator(pulse(12_002L, "ChampionKill", "assist", 6L)), NoTimers).toList
    outNext should have length 1
    outNext.head.momentumRaw should be > expected
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

  test("powerFrom handles None level/inv gracefully (returns 0)") {
    val p = newProc()
    p.powerFrom(None, None) shouldBe 0.0 +- 1e-9
  }

  test("snapshot with None level/inv and no other deltas does NOT emit") {
    val p = newProc()
    val s = HeatIn("GAME", "ORDER", "RIOT#ID", 1_000L, "snapshot",
      etype = None, championName = None, role = None,
      level = None, invValue = None, eventId = None,
      kills = Some(0), deaths = Some(0), assists = Some(0), creepScore = Some(0.0))
    val out = p.handleInputRows(testKey, Iterator(s), NoTimers).toList
    out shouldBe empty
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

  test("first snapshot emits and carries level (even if power ~ 0 at L1)") {
    val p = newProc()

    val out = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 1, 0.0, 0, 0, 0, 0.0, "")), NoTimers).toList
    out should have length 1
    out.head.level shouldBe 1
  }

  test("level monotone: lower level in later snapshot does not reduce level or emit") {
    val p = newProc()

    val s1 = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 6, 1000.0, 0, 0, 0, 20.0, "Wukong")), NoTimers).toList
    s1 should have length 1
    s1.head.level shouldBe 6

    // lower level + no beneficial deltas → merged state unchanged → no emit
    val s2 = p.handleInputRows(testKey, Iterator(snapshot(2_000L, 4, 900.0, 0, 0, 0, 15.0, "MonkeyKing")), NoTimers).toList
    s2 shouldBe empty
  }

  test("pulse before any snapshot: emission carries default level=0") {
    val p = newProc()

    val out = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "assist", 100L)), NoTimers).toList
    out should have length 1
    out.head.level shouldBe 0 // InitialState.level
  }

  test("pulse after snapshot: emission carries the latest known level") {
    val p = newProc()

    val s = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 5, 0.0, 0, 0, 0, 0.0, "Syndra")), NoTimers).toList
    s should have length 1
    s.head.level shouldBe 5

    val out = p.handleInputRows(testKey, Iterator(pulse(2_000L, "ChampionKill", "killer", 10L)), NoTimers).toList
    out should have length 1
    out.head.level shouldBe 5
  }

  test("level-only increase (same inv) emits and HeatOut.level increases") {
    val p = newProc()

    val s1 = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 1, 0.0, 0, 0, 0, 0.0, "")), NoTimers).toList
    s1 should have length 1
    s1.head.level shouldBe 1
    val p1 = s1.head.powerNorm

    val s2 = p.handleInputRows(testKey, Iterator(snapshot(2_000L, 2, 0.0, 0, 0, 0, 0.0, "")), NoTimers).toList
    s2 should have length 1
    s2.head.level shouldBe 2
    s2.head.powerNorm should be > p1
  }

  test("inventory-only increase emits and preserves level") {
    val p = newProc()

    val s1 = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 6, 1000.0, 0, 0, 0, 0.0, "Lux")), NoTimers).toList
    s1 should have length 1
    s1.head.level shouldBe 6

    val s2 = p.handleInputRows(testKey, Iterator(snapshot(2_000L, 6, 2000.0, 0, 0, 0, 0.0, "Lux")), NoTimers).toList
    s2 should have length 1
    s2.head.level shouldBe 6
    s2.head.powerNorm should be > s1.head.powerNorm
  }

  test("multiple pulses after a snapshot do not change level") {
    val p = newProc()

    val s = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 7, 1500.0, 0, 0, 0, 0.0, "Ahri")), NoTimers).toList
    s should have length 1
    s.head.level shouldBe 7

    val o1 = p.handleInputRows(testKey, Iterator(pulse(2_000L, "ChampionKill", "assist", 11L)), NoTimers).toList
    o1 should have length 1
    o1.head.level shouldBe 7

    val o2 = p.handleInputRows(testKey, Iterator(pulse(3_500L, "ChampionKill", "killer", 12L)), NoTimers).toList
    o2 should have length 1
    o2.head.level shouldBe 7
  }

  test("victim pulse resets spree but level remains unchanged") {
    val p = newProc()

    val s = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 8, 2500.0, 0, 0, 0, 0.0, "Zed")), NoTimers).toList
    s.head.level shouldBe 8

    val kill = p.handleInputRows(testKey, Iterator(pulse(2_000L, "ChampionKill", "killer", 1L)), NoTimers).toList
    kill.head.spreeStreak shouldBe 1
    kill.head.level shouldBe 8

    val death = p.handleInputRows(testKey, Iterator(pulse(3_000L, "ChampionKill", "victim", 2L)), NoTimers).toList
    death.head.spreeStreak shouldBe 0
    death.head.level shouldBe 8
  }

  test("mixed micro-batch ordering by timestamp: final emission carries latest level") {
    val p = newProc()

    // Intentionally out of order in iterator; processor sorts by tsMillis internally.
    val rows = Iterator(
      pulse(5_000L, "ChampionKill", "assist", 50L), // t=5s
      snapshot(6_000L, 6, 1200.0, 0, 0, 1, 10.0, "Lee Sin") // t=6s
    )

    val out = p.handleInputRows(testKey, rows, NoTimers).toList
    out should have length 1
    out.head.level shouldBe 6
    out.head.championName shouldBe "Lee Sin"
  }

  test("champion alias change with level increase: emits once, keeps sticky name and updated level") {
    val p = newProc()

    val s1 = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 5, 1200.0, 0, 0, 0, 0.0, "Wukong")), NoTimers).toList
    s1 should have length 1
    s1.head.level shouldBe 5
    s1.head.championName shouldBe "Wukong"

    // New snapshot: higher level, different alias → name should remain "Wukong" (sticky), level should increase
    val s2 = p.handleInputRows(testKey, Iterator(snapshot(2_000L, 6, 1200.0, 0, 0, 0, 0.0, "MonkeyKing")), NoTimers).toList
    s2 should have length 1
    s2.head.level shouldBe 6
    s2.head.championName shouldBe "Wukong"
  }

  test("snapshot with identical values as last snapshot does NOT emit") {
    val p = newProc()
    p.handleInputRows(testKey, Iterator(snapshot(1_000L, 3, 900.0, 1, 1, 0, 25.0, "Lux")), NoTimers).toList should have length 1

    val same = snapshot(2_000L, 3, 900.0, 1, 1, 0, 25.0, "Lux")
    p.handleInputRows(testKey, Iterator(same), NoTimers).toList shouldBe empty
  }

  test("snapshot: kills-only increase emits and merges monotone scoreboard") {
    val p = newProc()
    p.handleInputRows(testKey, Iterator(snapshot(1_000L, 3, 500.0, 0, 0, 0, 10.0, "Ashe")), NoTimers).toList

    val s2 = snapshot(2_000L, 3, 500.0, 1, 0, 0, 10.0, "Ashe")
    val out = p.handleInputRows(testKey, Iterator(s2), NoTimers).toList
    out should have length 1
    out.head.kills shouldBe 1
    out.head.deaths shouldBe 0
    out.head.assists shouldBe 0
  }

  test("snapshot: assists-only increase emits") {
    val p = newProc()
    p.handleInputRows(testKey, Iterator(snapshot(1_000L, 2, 300.0, 0, 0, 0, 5.0, "Ezreal")), NoTimers).toList

    val s2 = snapshot(2_000L, 2, 300.0, 0, 0, 1, 5.0, "Ezreal")
    val out = p.handleInputRows(testKey, Iterator(s2), NoTimers).toList
    out should have length 1
    out.head.kills shouldBe 0
    out.head.assists shouldBe 1
  }

  test("snapshot: deaths-only increase emits") {
    val p = newProc()
    p.handleInputRows(testKey, Iterator(snapshot(1_000L, 2, 300.0, 0, 0, 0, 5.0, "Ezreal")), NoTimers).toList

    val s2 = snapshot(2_000L, 2, 300.0, 0, 1, 0, 5.0, "Ezreal")
    val out = p.handleInputRows(testKey, Iterator(s2), NoTimers).toList
    out should have length 1
    out.head.deaths shouldBe 1
  }

  test("snapshot: creepScore-only increase emits") {
    val p = newProc()
    p.handleInputRows(testKey, Iterator(snapshot(1_000L, 2, 300.0, 0, 0, 0, 12.0, "Ezreal")), NoTimers).toList

    val s2 = snapshot(2_000L, 2, 300.0, 0, 0, 0, 25.0, "Ezreal")
    val out = p.handleInputRows(testKey, Iterator(s2), NoTimers).toList
    out should have length 1
    out.head.creepScore shouldBe 25.0
  }

  test("FirstBlood adds momentum but does not advance spree") {
    val p = newProc()
    val fb = HeatIn("GAME", "ORDER", "RIOT#ID", 10_000L, "pulse",
      etype = Some("FirstBlood"), championName = None, role = Some("killer"),
      level = None, invValue = None, eventId = Some(1L),
      kills = None, deaths = None, assists = None, creepScore = None)

    val out = p.handleInputRows(testKey, Iterator(fb), NoTimers).toList
    out should have length 1
    out.head.momentumRaw should be > 0.0
    out.head.spreeStreak shouldBe 0 // not a ChampionKill, so spree doesn't change
  }

  test("known type with missing role: decay-only anchor (no added momentum)") {
    val p = newProc()
    val k1 = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "assist", 1L)), NoTimers).toList
    val m0 = k1.head.momentumRaw

    // 45s later, same type but role=None, new eid → should emit with decayed baseline
    val anchor = HeatIn("GAME", "ORDER", "RIOT#ID", 55_000L, "pulse",
      etype = Some("ChampionKill"), championName = None, role = None,
      level = None, invValue = None, eventId = Some(2L),
      kills = None, deaths = None, assists = None, creepScore = None)

    val out = p.handleInputRows(testKey, Iterator(anchor), NoTimers).toList
    out should have length 1
    out.head.momentumRaw should be < m0
    out.head.spreeStreak shouldBe 0 // assist earlier didn’t change spree; missing role doesn’t either
  }

  test("dedupe inside one micro-batch: later smaller eventId is ignored") {
    val pA = newProc()
    // Two pulses same ts; eventId 5 then 4 → second should be ignored
    val batch = Iterator(
      pulse(10_000L, "ChampionKill", "killer", 5L),
      pulse(10_000L, "ChampionKill", "killer", 4L)
    )
    val outA = pA.handleInputRows(testKey, batch, NoTimers).toList
    outA should have length 1
    val mA = outA.head.momentumRaw

    val pB = newProc()
    val outB = pB.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "killer", 5L)), NoTimers).toList
    val mB = outB.head.momentumRaw

    mA shouldBe mB +- 1e-9
  }

  test("snapshot: lower level but higher inventory still emits; level remains monotone") {
    val p = newProc()
    val s1 = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 6, 1000.0, 0, 0, 0, 0.0, "Lux")), NoTimers).toList
    s1.head.level shouldBe 6
    val p1 = s1.head.powerNorm

    // Level goes down to 5 (should be ignored), inv rises (should emit and increase power)
    val s2 = p.handleInputRows(testKey, Iterator(snapshot(2_000L, 5, 2000.0, 0, 0, 0, 0.0, "Lux")), NoTimers).toList
    s2 should have length 1
    s2.head.level shouldBe 6
    s2.head.powerNorm should be > p1
  }

  test("two snapshots in one micro-batch: coalesced emit carries merged monotone scoreboard") {
    val p = newProc()
    val out = p.handleInputRows(
      testKey,
      Iterator(
        snapshot(10_000L, 4, 800.0, 0, 0, 0, 5.0, "Zeri"),
        snapshot(10_001L, 4, 800.0, 1, 0, 1, 10.0, "Zeri")
      ),
      NoTimers
    ).toList
    out should have length 1
    out.head.kills shouldBe 1
    out.head.assists shouldBe 1
    out.head.creepScore shouldBe 10.0
  }

  test("spree bonus caps: per-pulse contribution equal at cap and cap+1 (spacing = half-life)") {
    val p = newProc()

    // spacing so decay factor = 0.5 exactly
    val spacingMs = (HalfLifeSeconds * 1000L).toLong

    // kills needed to reach the spree bonus cap
    val killsToCap = math.ceil(SpreeCap / SpreeStep).toInt

    var eid = 0L

    def kill(ts: Long) = {
      eid += 1
      p.handleInputRows(testKey, Iterator(pulse(ts, "ChampionKill", "killer", eid)), NoTimers).toList.head
    }

    // build spree up to the cap
    var prev: HeatOut = kill(10_000L + spacingMs)
    var i = 2
    while (i <= killsToCap) {
      prev = kill(10_000L + i * spacingMs)
      i += 1
    }
    val atCap = prev

    val justAfterCap = p.handleInputRows(
      testKey, Iterator(pulse(10_000L + (killsToCap + 1) * spacingMs, "ChampionKill", "killer", {
        eid += 1; eid
      })), NoTimers
    ).toList.head
    val afterCapPlusOne = p.handleInputRows(
      testKey, Iterator(pulse(10_000L + (killsToCap + 2) * spacingMs, "ChampionKill", "killer", {
        eid += 1; eid
      })), NoTimers
    ).toList.head

    // contribution = m_k - 0.5 * m_{k-1}
    def contrib(curr: HeatOut, prev: HeatOut): Double = curr.momentumRaw - 0.5 * prev.momentumRaw

    val cAtCap = contrib(justAfterCap, atCap)
    val cAfterCap = contrib(afterCapPlusOne, justAfterCap)
    cAfterCap shouldBe cAtCap +- 1e-9

    // sanity: absolute deltas shrink due to decay on growing baseline
    val dAtCap = justAfterCap.momentumRaw - atCap.momentumRaw
    val dAfterCap = afterCapPlusOne.momentumRaw - justAfterCap.momentumRaw
    dAfterCap should be < dAtCap
  }

  test("half-life decay halves momentum after HalfLifeSeconds (using a no-op pulse to anchor emit)") {
    val p = newProc()

    // seed momentum
    val o1 = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "killer", 1L)), NoTimers).toList
    val m0 = o1.head.momentumRaw
    m0 should be > 0.0

    // after one half-life, emit a pulse with unknown type to surface the decayed baseline
    val o2 = p.handleInputRows(
      testKey,
      Iterator(pulse(10_000L + (HalfLifeSeconds * 1000L).toLong, "Noop", "assist", 2L)),
      NoTimers
    ).toList

    val mHalf = o2.head.momentumRaw
    mHalf shouldBe (m0 / 2.0) +- 1e-9
  }

  test("normalization bounds derive from constants: momentumNorm in [0,100], heat clamped, constants echoed") {
    val p = newProc()

    val out = p.handleInputRows(testKey, Iterator(
      pulse(10_000L, "BaronKill", "killer", 1L),
      pulse(10_050L, "BaronKill", "killer", 2L),
      pulse(10_100L, "BaronKill", "killer", 3L),
      pulse(10_150L, "BaronKill", "killer", 4L)
    ), NoTimers).toList

    out should have length 1
    val o = out.head

    o.momentumNorm should be >= 0.0
    o.momentumNorm should be <= 100.0
    o.heat should be >= 0.0
    o.heat should be <= 100.0

    // constants surfaced by the processor should echo the companion’s values
    o.momentumCap shouldBe MomentumCap +- 1e-9
    o.halfLifeSec shouldBe HalfLifeSeconds +- 1e-9
  }
}