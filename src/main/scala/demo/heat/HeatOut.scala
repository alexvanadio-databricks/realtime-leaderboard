package demo.heat

/** Single event emitted by TWS for the FE to render Heat with low latency.
 * Contains enough state + constants for client-side projection/decay between pulses.
 */
case class HeatOut(
                    gameId: String, // Game/session identifier; partitions state and joins with other data
                    team: String,
                    riotId: String, // Player identifier within the game; the second part of the key
                    championName: String,
                    isDead: Boolean,
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
                    items_str: String,
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
