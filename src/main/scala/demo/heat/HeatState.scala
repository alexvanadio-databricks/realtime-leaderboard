package demo.heat

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
                      isDead: Boolean,
                      level: Int,
                      items_str: String
                    )