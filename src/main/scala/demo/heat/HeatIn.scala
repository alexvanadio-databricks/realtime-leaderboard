package demo.heat

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
                   creepScore: Option[Double],
                   items_str: Option[String], // string encoded items in the format "[slot]:[itemId]:[displayName], again, .."
                   isDead: Option[Boolean]
                 )

