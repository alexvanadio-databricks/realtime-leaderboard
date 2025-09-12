// NOTE: we need this until because of some messy Databricks internals and wrappers. 
// It would be better to move this entire notebook to normal source code and then make a jar
package demo.heat

import demo.heat.HeatProcessor.InitialState
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming._

// input to TWS after your select into a single stream
case class HeatIn(
                   gameId: String,
                   team: String,
                   riotId: String,
                   tsMillis: Long,
                   kind: String, // "snapshot" | "pulse"
                   etype: Option[String], // pulses only: "ChampionKill", "DragonKill", ...
                   championName: Option[String],
                   role: Option[String], // pulses only: "killer" | "assist" | "victim"
                   level: Option[Int], // snapshots only
                   invValue: Option[Double], // snapshots only (already computed upstream)
                   eventId: Option[Long], // pulses may have this
                   kills: Option[Int],
                   deaths: Option[Int],
                   assists: Option[Int],
                   creepScore: Option[Double]
                 )

case class HeatState(
                      p: Double, // smoothed power 0..100
                      m: Double, // momentum accumulator (>=0)
                      lastTs: Long,
                      lastEventId: Long, // minimal per-key dedupe

                      // bonuses state
                      spreeStreak: Int, // consecutive kills since last death

                      kills: Int,
                      deaths: Int,
                      assists: Int,
                      creepScore: Double,

                      invValue: Double,
                      championName: String,
                      level: Int
                    )

/** Single event emitted by TWS for the FE to render Heat with low latency.
 * Contains enough state + constants for client-side projection/decay between pulses.
 */
case class HeatOut(
                    gameId: String, // Game/session identifier; partitions state and joins with other data
                    team: String,
                    riotId: String, // Player identifier within the game; the second part of the key
                    championName: String,
                    emitTsMs: Long, // Server-side epoch ms when this record was emitted (anchor for FE projection)
                    powerRaw: Double, // Power at emit time, normalized to [0..100] (from level/items)
                    momentumRaw: Double, // Momentum accumulator at emit time (raw/unbounded, pre-normalization)
                    powerNorm: Double,
                    momentumNorm: Double,
                    heat: Double,

                    // bonuses state
                    spreeStreak: Int, // consecutive kills since last death

                    // scores
                    kills: Int,
                    deaths: Int,
                    assists: Int,
                    creepScore: Double,

                    invValue: Double,
                    level: Int,

                    // ---- constants the FE needs (kept in payload for transparency/versioning) ----
                    halfLifeSec: Double, // Momentum half-life in seconds for exponential decay on FE
                    momentumCap: Double,
                    heatWPower: Double, // Weight of Power in Heat: heat = p*heatWPower + momentumNorm*heatWMomentum
                    heatWMomentum: Double, // Weight of normalized Momentum in Heat
                    itemBudget: Double, // “Full build” gold used to normalize inventory value to [0..100]
                    levelMax: Double, // Max champion level used to normalize level to [0..100]

                    // ---- provenance & latency ----
                    sourceTsMs: Long, // Epoch ms of the input record that produced this output (producer timestamp)
                    backendLatencyMs: Long // (emitTsMs - sourceTsMs), clamped to >= 0; backend processing latency only
                  )

/** Minimal state accessor the processor uses instead of directly touching Spark ValueState. */
trait HeatStateStore {
  def get(): HeatState
  def update(s: HeatState): Unit
}

/** Simple in-memory store for unit tests. */
final class InMemoryHeatStateStore(init: HeatState) extends HeatStateStore {
  private[this] var v: HeatState = init
  override def get(): HeatState = v
  override def update(s: HeatState): Unit = { v = s }
}

object HeatProcessor {
  val InitialState: HeatState = HeatState(
    p = 0.0, m = 0.0, lastTs = 0L, lastEventId = (-1L), spreeStreak = 0, level = 0,
    kills = 0, deaths = 0, assists = 0, creepScore = 0.0, invValue = 0.0, championName = ""
  )

  // ----- POWER (simple: level + inventory value) -----
  val PowerWLevel = 0.55
  val PowerWItems = 0.45
  val LevelMax = 18.0
  val ItemBudget = 16500.0 // late game high-end inventory appears to be worth about this much

  // ----- MOMENTUM (leaky bucket) -----
  val HalfLifeSeconds = 45.0
  val MomentumCap = 130.0
  val AssistFactor = 0.7
  val SpreeStep = 2.0 // +2 per kill in current life
  val SpreeCap = 16.0 // cap of spree bonus

  // Base pulse weights
  val W_Kill = 14.0
  val W_Assist = 8.0
  val W_Death = -6.0
  val W_FirstBlood = 6.0
  val W_Dragon = 16.0
  val W_Baron = 22.0
  val W_Herald = 10.0
  val W_Turret = 9.0

  val HeatWPower = 0.45
  val HeatWMomentum = 0.55

  val momHLms = HalfLifeSeconds * 1000.0
}

class HeatProcessor(ttl: TTLConfig, injectedStore: Option[HeatStateStore] = None)
  extends StatefulProcessor[String, HeatIn, HeatOut] {

  import HeatProcessor._

  @transient private var _sparkValueState: ValueState[HeatState] = _
  @transient private var store: HeatStateStore = injectedStore.orNull

  override def init(out: OutputMode, tm: TimeMode): Unit = {
    // If tests injected a store, skip touching Spark’s handle.
    if (store != null) return

    _sparkValueState = getHandle.getValueState("heatState", Encoders.product[HeatState], ttl)
    // Wrap Spark ValueState behind our tiny trait
    store = new HeatStateStore {
      override def get(): HeatState = Option(_sparkValueState.get()).getOrElse(InitialState)
      override def update(s: HeatState): Unit = _sparkValueState.update(s)
    }
  }

  @inline def decay(value: Double, hlMs: Double, dtMs: Long): Double =
    if (dtMs <= 0L) value else value * math.pow(0.5, dtMs / hlMs)

  def powerFrom(level: Option[Int], inv: Option[Double]): Double = {
    val nL = level.map(l => ((l - 1).toDouble / (LevelMax - 1)).max(0).min(1)).getOrElse(0.0)
    val nI = inv.map(v => (v / ItemBudget).max(0).min(1)).getOrElse(0.0)
    100.0 * (PowerWLevel * nL + PowerWItems * nI)
  }

  private def splitKey(k: String): (String, String, String) = {
    val parts = k.split("\\|", 3)
    if (parts.length == 3) (parts(0), parts(1), parts(2)) else (k, "", "")
  }

  private[heat] case class StepFlags(changedSnapshot: Boolean, changedPulse: Boolean)

  private[heat] def step(st: HeatState, heatIn: HeatIn): (HeatState, StepFlags) = {
    val kind = heatIn.kind
    val ts = heatIn.tsMillis
    val dt = math.max(0L, ts - st.lastTs)
    val mDec = decay(st.m, momHLms, dt)

    kind match {
      case "pulse" =>
        val eventId = heatIn.eventId
        val etype = heatIn.etype
        val role = heatIn.role

        var spreeNow = st.spreeStreak
        var bonus = 0.0

        if (eventId.nonEmpty && eventId.get <= st.lastEventId)
          return (st.copy(
            m = mDec,
            lastTs = math.max(st.lastTs, ts)),
            StepFlags(changedSnapshot = false, changedPulse = false)
          )

        val base = (etype, role) match {
          case (Some("ChampionKill"), Some("killer")) => W_Kill
          case (Some("ChampionKill"), Some("assist")) => W_Assist
          case (Some("ChampionKill"), Some("victim")) => W_Death
          case (Some("FirstBlood"), Some("killer")) => W_FirstBlood
          case (Some("DragonKill"), Some("killer")) => W_Dragon
          case (Some("DragonKill"), Some("assist")) => AssistFactor * W_Dragon
          case (Some("BaronKill"), Some("killer")) => W_Baron
          case (Some("BaronKill"), Some("assist")) => AssistFactor * W_Baron
          case (Some("HeraldKill"), Some("killer")) => W_Herald
          case (Some("HeraldKill"), Some("assist")) => AssistFactor * W_Herald
          case (Some("TurretKilled"), Some("killer")) => W_Turret
          case (Some("TurretKilled"), Some("assist")) => AssistFactor * W_Turret
          case _ => 0.0
        }

        // --- SPREE ONLY (no multikill) ---
        if (etype.contains("ChampionKill") && role.contains("killer")) {
          spreeNow += 1
          bonus += math.min(SpreeCap, SpreeStep * spreeNow)
        }
        if (etype.contains("ChampionKill") && role.contains("victim"))
          spreeNow = 0

        val rawMomentum = base + bonus
        val momentumNow = math.min(MomentumCap, math.max(0.0, mDec + rawMomentum))

        val updatedHeatState = st.copy(m=momentumNow, lastTs = math.max(st.lastTs, ts), spreeStreak = spreeNow)

        eventId match {
          case Some(eventId: Long) =>
            (updatedHeatState.copy(lastEventId = eventId), StepFlags(changedSnapshot = false, changedPulse = true))
          case None =>
            (updatedHeatState, StepFlags(changedSnapshot = false, changedPulse = true))
        }

      case "snapshot" =>
        val levelOpt = heatIn.level
        val invOpt = heatIn.invValue

        // scoreboard (use provided or fallback to state, then monotone max merge)
        val killsIn = heatIn.kills.getOrElse(st.kills)
        val deathsIn = heatIn.deaths.getOrElse(st.deaths)
        val assistsIn = heatIn.assists.getOrElse(st.assists)
        val csIn = heatIn.creepScore.getOrElse(st.creepScore)
        val champIn = heatIn.championName.getOrElse("")

        // --- monotone merges (never decrease from snapshots) ---
        val levelNew = math.max(st.level, levelOpt.getOrElse(st.level))
        val invNew = math.max(st.invValue, invOpt.getOrElse(st.invValue))
        val kNew = math.max(st.kills, killsIn)
        val dNew = math.max(st.deaths, deathsIn)
        val aNew = math.max(st.assists, assistsIn)
        val csNew = math.max(st.creepScore, csIn)

        // sticky champion name (set once, then keep)
        val champNew = if (st.championName.nonEmpty) st.championName else champIn

        // compute power FROM MERGED STATE (prevents accidental drops)
        val pNew = powerFrom(Some(levelNew), Some(invNew))

        // persist decayed momentum baseline and merged fields
        val st2 = st.copy(
          m = mDec,
          p = pNew,
          level = levelNew,
          invValue = invNew,
          kills = kNew, deaths = dNew, assists = aNew,
          creepScore = csNew, championName = champNew,
          lastTs = math.max(st.lastTs, ts)
        )

        // change detection AFTER merge
        val levelChanged = levelNew > st.level
        val pChanged = math.abs(pNew - st.p) > 1e-6
        val kChanged = kNew > st.kills
        val dChanged = dNew > st.deaths
        val aChanged = aNew > st.assists
        val csChanged = csNew > st.creepScore
        val invChanged = invNew > st.invValue
        val champChanged = st.championName.isEmpty && champNew.nonEmpty

        val snapshotChanged =
          levelChanged || pChanged || kChanged || dChanged || aChanged || csChanged || invChanged || champChanged

        (st2, StepFlags(changedSnapshot = snapshotChanged, changedPulse = false))
    }
  }

  private def singleHeatOut(key: String, st: HeatState): HeatOut = {
    // identity
    val (gameIdK, riotIdK, teamK) = splitKey(key)

    // normalization
    val nP = math.max(0.0, math.min(100.0, st.p)) // power already on 0..100
    val nM = math.max(0.0, math.min(100.0, 100.0 * st.m / MomentumCap))

    // blended heat (clamped)
    val heatVal = math.max(0.0, math.min(100.0, HeatWPower * nP + HeatWMomentum * nM))

    // provenance / latency
    val emitTsMs = System.currentTimeMillis()
    val sourceTsMs = st.lastTs
    val backendLatencyMs = math.max(0L, emitTsMs - sourceTsMs)

    HeatOut(
      gameId = gameIdK,
      team = teamK,
      riotId = riotIdK,
      championName = st.championName,

      emitTsMs = emitTsMs,

      powerRaw = st.p,
      momentumRaw = st.m,
      powerNorm = nP,
      momentumNorm = nM,
      heat = heatVal,

      spreeStreak = st.spreeStreak,

      kills = st.kills,
      deaths = st.deaths,
      assists = st.assists,
      creepScore = st.creepScore,

      invValue = st.invValue,
      level = st.level,

      // FE constants
      halfLifeSec = HalfLifeSeconds,
      momentumCap = MomentumCap,
      heatWPower = HeatWPower,
      heatWMomentum = HeatWMomentum,
      itemBudget = ItemBudget,
      levelMax = LevelMax,

      // provenance
      sourceTsMs = sourceTsMs,
      backendLatencyMs = backendLatencyMs
    )
  }

  override def handleInputRows(
                                key: String,
                                inputRows: Iterator[HeatIn],
                                timers: TimerValues): Iterator[HeatOut] = {

    // pull current state (or default)
    var st = Option(store.get()).getOrElse(InitialState)

    // OR-accumulate what changed across all rows in this micro-batch
    var changedPulse = false
    var changedSnapshot = false

    // ensure per-key time order; step() uses dt = max(0, ts - lastTs)
    val rows = inputRows.toArray.sortBy(_.tsMillis)

    var i = 0
    while (i < rows.length) {
      val r = rows(i)

      // --- the seam: one row -> (newState, flags) ---
      val (st2, flags) = step(st, r)

      // accumulate and advance
      changedPulse ||= flags.changedPulse
      changedSnapshot ||= flags.changedSnapshot
      st = st2

      i += 1
    }

    // persist final state for this key
    store.update(st)

    // emit only if something material changed (pulse accepted or snapshot fields advanced)
    if (changedPulse || changedSnapshot) Iterator(singleHeatOut(key, st))
    else Iterator.empty
  }
}

