package demo.heat

/** Simple in-memory store for unit tests. */
final class InMemoryHeatStateStore(init: HeatState) extends HeatStateStore {
  private[this] var v: HeatState = init
  override def get(): HeatState = v
  override def update(s: HeatState): Unit = { v = s }
}