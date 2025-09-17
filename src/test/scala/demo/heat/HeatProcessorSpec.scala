package demo.heat

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.streaming._ // for OutputMode, TimeMode
import java.time.Duration
import HeatProcessor._

final class TestClock(var current: Long) { def now(): Long = current }

final class HeatProcessorSpec extends AnyFunSuite with Matchers {

  private val testKey = "GAME|RIOT#ID|ORDER"

  // Use this for deterministic time in tests
  private def newProcAt(t0: Long): (HeatProcessor, TestClock) = {
    val clk = new TestClock(t0)
    val p = new HeatProcessor(
      ttl = TTLConfig(Duration.ofHours(2)),
      injectedStore = Some(new InMemoryHeatStateStore(InitialState)),
      nowFn = clk.now _
    )
    (p, clk)
  }

  // Minimal helper if you don’t need clock control for a test
  private def newProc(): HeatProcessor =
    new HeatProcessor(
      ttl = TTLConfig(Duration.ofHours(2)),
      injectedStore = Some(new InMemoryHeatStateStore(InitialState)),
      nowFn = () => System.currentTimeMillis()
    )

  // ---- helpers to build HeatIn -------------------------------------------------
  private def pulse(ts: Long, et: String, role: String, eid: Long) =
    HeatIn("GAME", "ORDER", "RIOT#ID", ts, "pulse", Some(et), None, Some(role),
      None, None, Some(eid), None, None, None, None, None)

  private def pulseNoEid(ts: Long, et: String, role: String) =
    HeatIn("GAME", "ORDER", "RIOT#ID", ts, "pulse", Some(et), None, Some(role),
      None, None, None, None, None, None, None, None)

  private def snapshot(ts: Long, level: Int, inv: Double, k: Int, d: Int, a: Int, cs: Double, champ: String) =
    HeatIn("GAME", "ORDER", "RIOT#ID", ts, "snapshot", None, Some(champ), None,
      Some(level), Some(inv), None, Some(k), Some(d), Some(a), Some(cs), None)

  private def snapshotWithItems(ts: Long, level: Int, inv: Double, k: Int,
                                d: Int, a: Int, cs: Double, champ: String, items: String) =
    HeatIn("GAME", "ORDER", "RIOT#ID", ts, "snapshot", None, Some(champ), None,
      Some(level), Some(inv), None, Some(k), Some(d), Some(a), Some(cs), Some(items))

  // No timers used by our code; pass null safely.
  private val NoTimers: TimerValues = null.asInstanceOf[TimerValues]

  // Small utility for expected decay
  @inline private def decayFrom(m: Double, dtMs: Long): Double =
    m * math.pow(0.5, dtMs / (HalfLifeSeconds * 1000.0))

  // ------------------------------------------------------------------------------
  // Core momentum + dedupe + ordering
  // ------------------------------------------------------------------------------

  test("assist yields momentum; duplicate eid emits a purely decayed frame; next eid increases momentum") {
    val (p, clk) = newProcAt(10_000L)

    clk.current = 10_000L
    val out1 = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "assist", 100L)), NoTimers).toList
    val m1 = out1.head.momentumRaw
    m1 should be > 0.0

    clk.current = 12_000L
    val outDup = p.handleInputRows(testKey, Iterator(pulse(12_000L, "ChampionKill", "assist", 100L)), NoTimers).toList
    outDup should have length 1
    outDup.head.momentumRaw shouldBe decayFrom(m1, 2000L) +- 1e-9  // deduped pulse → decay only

    clk.current = 14_000L
    val out2 = p.handleInputRows(testKey, Iterator(pulse(14_000L, "ChampionKill", "assist", 101L)), NoTimers).toList
    out2 should have length 1
    out2.head.momentumRaw should be > outDup.head.momentumRaw
  }

  test("out-of-order timestamp is accepted when eventId increases (dt=0 path)") {
    val (p, clk) = newProcAt(10_000L)

    clk.current = 10_000L
    val o1 = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "assist", 10L)), NoTimers).toList
    val mBefore = o1.head.momentumRaw

    // older ts but larger eid → accepted; dt=max(0, 9000-10000)=0 → no decay, only add base
    clk.current = 9_000L
    val o2 = p.handleInputRows(testKey, Iterator(pulse(9_000L, "ChampionKill", "assist", 11L)), NoTimers).toList
    o2 should have length 1
    o2.head.momentumRaw should be > mBefore
  }

  test("micro-batch sorts by ts: reversed two-pulse batch has same FINAL momentum as two ascending batches") {
    val (pA, clkA) = newProcAt(10_000L)
    // One batch, reversed iterator order → processor sorts internally; we emit per-row, compare the final frame
    val outA = {
      clkA.current = 12_000L
      pA.handleInputRows(testKey,
        Iterator(
          pulse(12_000L, "ChampionKill", "assist", 2L),
          pulse(10_000L, "ChampionKill", "assist", 1L)
        ),
        NoTimers
      ).toList
    }
    outA should have length 2
    val mAfinal = outA.last.momentumRaw

    val (pB, clkB) = newProcAt(10_000L)
    clkB.current = 10_000L
    val b1 = pB.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "assist", 1L)), NoTimers).toList
    clkB.current = 12_000L
    val b2 = pB.handleInputRows(testKey, Iterator(pulse(12_000L, "ChampionKill", "assist", 2L)), NoTimers).toList
    val mBfinal = b2.last.momentumRaw

    mAfinal shouldBe mBfinal +- 1e-9
  }

  test("half-life decay halves momentum after 45s (using a no-op pulse as an emit anchor)") {
    val (p, clk) = newProcAt(10_000L)

    clk.current = 10_000L
    val o1 = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "killer", 1L)), NoTimers).toList
    val m0 = o1.head.momentumRaw
    m0 should be > 0.0

    clk.current = 10_000L + (HalfLifeSeconds * 1000L).toLong
    val o2 = p.handleInputRows(testKey, Iterator(pulse(clk.current, "Noop", "assist", 2L)), NoTimers).toList
    o2.head.momentumRaw shouldBe (m0 / 2.0) +- 1e-9
  }

  test("momentum caps: repeated big positives saturate momentumNorm to 100") {
    val (p, clk) = newProcAt(10_000L)
    var eid = 1L
    var last = 0.0
    (0 until 10).foreach { i =>
      val t = 10_000L + i
      clk.current = t
      val o = p.handleInputRows(testKey, Iterator(pulse(t, "BaronKill", "killer", { eid += 1; eid })), NoTimers).toList
      last = o.last.momentumNorm
    }
    last shouldBe 100.0 +- 1e-4
  }

  test("assist vs killer on DragonKill: assist adds less raw momentum than killer") {
    val (p1, c1) = newProcAt(10_000L)
    c1.current = 10_000L
    val a = p1.handleInputRows(testKey, Iterator(pulse(10_000L, "DragonKill", "assist", 1L)), NoTimers).toList.head.momentumRaw

    val (p2, c2) = newProcAt(10_000L)
    c2.current = 10_000L
    val k = p2.handleInputRows(testKey, Iterator(pulse(10_000L, "DragonKill", "killer", 1L)), NoTimers).toList.head.momentumRaw

    a should be < k
  }

  // ------------------------------------------------------------------------------
  // Spree + death behavior
  // ------------------------------------------------------------------------------

  test("spree increments on killer, resets on victim; death cannot drive momentum below zero") {
    val (p, clk) = newProcAt(5_000L)

    clk.current = 5_000L
    val o1 = p.handleInputRows(testKey, Iterator(pulse(5_000L,"ChampionKill","killer",1L)), NoTimers).toList
    o1.head.spreeStreak shouldBe 1

    clk.current = 8_000L
    val o2 = p.handleInputRows(testKey, Iterator(pulse(8_000L,"ChampionKill","killer",2L)), NoTimers).toList
    o2.head.spreeStreak shouldBe 2
    o2.head.momentumRaw should be > 0.0

    clk.current = 10_000L
    val o3 = p.handleInputRows(testKey, Iterator(pulse(10_000L,"ChampionKill","victim",3L)), NoTimers).toList
    o3.head.spreeStreak shouldBe 0
    o3.head.momentumRaw should be >= 0.0
  }

  // ------------------------------------------------------------------------------
  // Snapshot semantics: monotone merges, sticky champion, always-emit
  // ------------------------------------------------------------------------------

  test("snapshot persists decayed baseline; monotone merge of scoreboard/inv; sticky champion; always emits") {
    val (p, clk) = newProcAt(10_000L)

    // Seed momentum
    clk.current = 10_000L
    val outPulse = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "killer", 10L)), NoTimers).toList
    val m0 = outPulse.head.momentumRaw

    // Snapshot with deltas
    clk.current = 55_000L
    val s1 = p.handleInputRows(testKey,
      Iterator(snapshot(55_000L, 6, 2000.0, 2, 1, 3, 80.0, "Wukong")), NoTimers
    ).toList
    s1 should have length 1
    s1.head.momentumRaw should be < m0
    s1.head.kills shouldBe 2
    s1.head.deaths shouldBe 1
    s1.head.assists shouldBe 3
    s1.head.creepScore shouldBe 80.0
    s1.head.invValue shouldBe 2000.0
    s1.head.level shouldBe 6
    s1.head.championName shouldBe "Wukong"

    // Lower cs/inv; new alias; same level → state should remain monotone but still emit (decayed)
    clk.current = 56_000L
    val s2 = p.handleInputRows(testKey,
      Iterator(snapshot(56_000L, 6, 1500.0, 2, 1, 3, 70.0, "MonkeyKing")), NoTimers
    ).toList
    s2 should have length 1
    s2.head.kills shouldBe 2
    s2.head.deaths shouldBe 1
    s2.head.assists shouldBe 3
    s2.head.creepScore shouldBe 80.0
    s2.head.invValue shouldBe 2000.0 // monotone
    s2.head.level shouldBe 6
    s2.head.championName shouldBe "Wukong" // sticky
    s2.head.momentumRaw should be <= s1.head.momentumRaw
  }

  test("level monotone: lower level in later snapshot does not reduce level (still emits)") {
    val (p, clk) = newProcAt(1_000L)

    clk.current = 1_000L
    val s1 = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 6, 1000.0, 0, 0, 0, 20.0, "Wukong")), NoTimers).toList
    s1.head.level shouldBe 6

    // lower level + lower inv → no regression; still emits decayed frame
    clk.current = 2_000L
    val s2 = p.handleInputRows(testKey, Iterator(snapshot(2_000L, 4, 900.0, 0, 0, 0, 15.0, "MonkeyKing")), NoTimers).toList
    s2 should have length 1
    s2.head.level shouldBe 6
    s2.head.invValue shouldBe 1000.0 +- 1e-9
  }

  test("pulse carries level from latest snapshot; pulses alone don’t change level") {
    val (p, clk) = newProcAt(1_000L)

    clk.current = 1_000L
    val s = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 5, 0.0, 0, 0, 0, 0.0, "Syndra")), NoTimers).toList
    s.head.level shouldBe 5

    clk.current = 2_000L
    val out = p.handleInputRows(testKey, Iterator(pulse(2_000L, "ChampionKill", "killer", 10L)), NoTimers).toList
    out.head.level shouldBe 5
  }

  // ------------------------------------------------------------------------------
  // Items: set, change, ignore older
  // ------------------------------------------------------------------------------

  test("first snapshot with items sets items_str; items-only change emits; older-ts items ignored") {
    val (p, clk) = newProcAt(1_000L)
    val itemsA = "0:100:Boots of Speed, 1:200:Sword of Might"
    val itemsB = "0:100:Boots of Speed, 1:201:Sword of Justice"

    // set items
    clk.current = 1_000L
    val s1 = p.handleInputRows(testKey, Iterator(snapshotWithItems(1_000L, 6, 2000.0, 0, 0, 0, 0.0, "Lux", itemsA)), NoTimers).toList
    s1.head.items_str shouldBe itemsA

    // items-only change
    clk.current = 2_000L
    val s2 = p.handleInputRows(testKey, Iterator(snapshotWithItems(2_000L, 6, 2000.0, 0, 0, 0, 0.0, "Lux", itemsB)), NoTimers).toList
    s2 should have length 1
    s2.head.items_str shouldBe itemsB
    s2.head.level shouldBe 6
    s2.head.invValue shouldBe 2000.0 +- 1e-9

    // older snapshot with different items should NOT override (ts < lastTs), still emits decayed frame but keeps itemsB
    clk.current = 2_500L
    val sOld = p.handleInputRows(testKey, Iterator(snapshotWithItems(1_500L, 6, 2000.0, 0, 0, 0, 0.0, "Lux", "0:100:Boots")), NoTimers).toList
    sOld should have length 1
    sOld.head.items_str shouldBe itemsB

    // next pulse should still carry itemsB
    clk.current = 3_000L
    val pOut = p.handleInputRows(testKey, Iterator(pulse(3_000L, "ChampionKill", "assist", 42L)), NoTimers).toList
    pOut.head.items_str shouldBe itemsB
  }

  // ------------------------------------------------------------------------------
  // Bounds, routing, roles, and missing eid
  // ------------------------------------------------------------------------------

  test("normalization bounds and constants: momentumNorm, heat ∈ [0,100]; constants echoed; backendLatency >= 0") {
    val (p, clk) = newProcAt(10_000L)
    clk.current = 10_000L
    val out = p.handleInputRows(testKey, Iterator(
      pulse(10_000L, "BaronKill", "killer", 1L),
      pulse(10_050L, "BaronKill", "killer", 2L),
      pulse(10_100L, "BaronKill", "killer", 3L),
      pulse(10_150L, "BaronKill", "killer", 4L)
    ), NoTimers).toList

    out should have length 4
    val o = out.last
    o.momentumNorm should be >= 0.0
    o.momentumNorm should be <= 100.0
    o.heat should be >= 0.0
    o.heat should be <= 100.0
    o.backendLatencyMs should be >= 0L
    o.sourceTsMs should be > 0L
    o.momentumCap shouldBe MomentumCap +- 1e-9
    o.halfLifeSec shouldBe HalfLifeSeconds +- 1e-9
  }

  test("key routing: gameId/team/riotId are taken from the composite key") {
    val (p, clk) = newProcAt(1_000L)
    val key2 = "G1|Summ#EU|CHAOS"

    clk.current = 1_000L
    val out = p.handleInputRows(key2, Iterator(snapshot(1_000L, 3, 500.0, 0, 0, 0, 0.0, "Syndra")), NoTimers).toList
    out.head.gameId shouldBe "G1"
    out.head.riotId shouldBe "Summ#EU"
    out.head.team shouldBe "CHAOS"
  }

  test("known type with missing role anchors a decayed emit (no added momentum)") {
    val (p, clk) = newProcAt(10_000L)

    clk.current = 10_000L
    val k1 = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "assist", 1L)), NoTimers).toList
    val m0 = k1.head.momentumRaw

    clk.current = 55_000L
    val anchor = HeatIn("GAME", "ORDER", "RIOT#ID", 55_000L, "pulse",
      etype = Some("ChampionKill"), championName = None, role = None,
      level = None, invValue = None, eventId = Some(2L),
      kills = None, deaths = None, assists = None, creepScore = None, items_str = None)

    val out = p.handleInputRows(testKey, Iterator(anchor), NoTimers).toList
    out.head.momentumRaw shouldBe decayFrom(m0, 45_000L) +- 1e-9
    out.head.spreeStreak shouldBe 0
  }

  test("pulses with missing eventId are not deduped: two emits and cumulative momentum") {
    val (p, clk) = newProcAt(10_000L)

    clk.current = 10_000L
    val a1 = p.handleInputRows(testKey, Iterator(pulseNoEid(10_000L, "ChampionKill", "assist")), NoTimers).toList
    val m1 = a1.head.momentumRaw

    clk.current = 12_000L
    val a2 = p.handleInputRows(testKey, Iterator(pulseNoEid(12_000L, "ChampionKill", "assist")), NoTimers).toList
    a2.head.momentumRaw should be > m1
  }

  // --- Additions to HeatProcessorSpec ------------------------------------------------

  test("dedupe in one micro-batch: later smaller eventId still emits; second frame equals decayed baseline (dt=0)") {
    val (p, clk) = newProcAt(10_000L)
    clk.current = 10_000L
    val out = p.handleInputRows(
      testKey,
      Iterator(
        pulse(10_000L, "ChampionKill", "killer", 5L),
        pulse(10_000L, "ChampionKill", "killer", 4L) // deduped by eid
      ),
      NoTimers
    ).toList

    out should have length 2
    out(1).momentumRaw shouldBe out(0).momentumRaw +- 1e-9 // dt=0 → no extra decay/gain
    out(1).spreeStreak shouldBe out(0).spreeStreak
  }

  test("heat equals HeatWPower * powerNorm when momentum is zero") {
    val (p, clk) = newProcAt(1_000L)
    clk.current = 1_000L
    val o = p.handleInputRows(testKey, Iterator(snapshot(1_000L, 6, 2000.0, 0, 0, 0, 0.0, "Lux")), NoTimers).toList.head
    o.momentumRaw shouldBe 0.0 +- 1e-9
    val expectedHeat = HeatWPower * o.powerNorm
    o.heat shouldBe expectedHeat +- 1e-9
  }

  test("power normalization saturates at 100 when level=LevelMax and inv>=ItemBudget") {
    val (p, clk) = newProcAt(1_000L)
    clk.current = 1_000L
    val o = p.handleInputRows(testKey, Iterator(snapshot(1_000L, LevelMax.toInt, ItemBudget, 0, 0, 0, 0.0, "Lux")), NoTimers).toList.head
    o.powerNorm shouldBe 100.0 +- 1e-9
    o.powerRaw shouldBe 100.0 +- 1e-9
  }

  test("same-timestamp, increasing eventId: second pulse accepted with dt=0 and momentum increases") {
    val (p, clk) = newProcAt(10_000L)
    clk.current = 10_000L

    val out = p.handleInputRows(
      testKey,
      Iterator(
        pulse(10_000L, "ChampionKill", "assist", 1L),
        pulse(10_000L, "ChampionKill", "assist", 2L) // larger eid, same ts → dt=0
      ),
      NoTimers
    ).toList

    out should have length 2
    out(1).momentumRaw should be > out(0).momentumRaw // additive base, no decay
  }

  test("whitespace-only items string is ignored (no override); still emits and retains previous items") {
    val (p, clk) = newProcAt(1_000L)
    val itemsA = "0:100:Boots of Speed, 1:200:Sword of Might"

    clk.current = 1_000L
    p.handleInputRows(testKey, Iterator(
      snapshotWithItems(1_000L, 4, 1200.0, 0, 0, 0, 0.0, "Jinx", itemsA)
    ), NoTimers).toList should have length (1)

    // whitespace-only → treated as missing; emit occurs (always-emit), but items don't change
    clk.current = 2_000L
    val out = p.handleInputRows(testKey, Iterator(
      snapshotWithItems(2_000L, 4, 1200.0, 0, 0, 0, 0.0, "Jinx", "   ")
    ), NoTimers).toList

    out should have length 1
    out.head.items_str shouldBe itemsA
  }

  test("repeated deaths clamp momentum at zero and keep spree at zero") {
    val (p, clk) = newProcAt(10_000L)

    // seed a little momentum
    clk.current = 10_000L
    p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "assist", 1L)), NoTimers).toList.head.momentumRaw should be > 0.0

    // two deaths back-to-back
    clk.current = 11_000L
    val d1 = p.handleInputRows(testKey, Iterator(pulse(11_000L, "ChampionKill", "victim", 2L)), NoTimers).toList.head
    d1.momentumRaw should be >= 0.0
    d1.spreeStreak shouldBe 0

    clk.current = 12_000L
    val d2 = p.handleInputRows(testKey, Iterator(pulse(12_000L, "ChampionKill", "victim", 3L)), NoTimers).toList.head
    d2.momentumRaw shouldBe 0.0 +- 1e-9
    d2.spreeStreak shouldBe 0
  }

  test("momentum decays to near-zero after several half-lives using no-op anchor pulses") {
    val (p, clk) = newProcAt(10_000L)

    clk.current = 10_000L
    val o1 = p.handleInputRows(testKey, Iterator(pulse(10_000L, "ChampionKill", "killer", 1L)), NoTimers).toList.head
    val m0 = o1.momentumRaw

    // Advance 4 half-lives → expected ≈ m0 * (1/16)
    val t = 10_000L + (4 * HalfLifeSeconds * 1000L).toLong
    clk.current = t
    val o2 = p.handleInputRows(testKey, Iterator(pulse(t, "Noop", "assist", 2L)), NoTimers).toList.head
    val expected = m0 / 16.0
    o2.momentumRaw shouldBe expected +- (m0 * 1e-6) // tight tolerance
  }
}
