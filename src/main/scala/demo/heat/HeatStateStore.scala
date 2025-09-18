package demo.heat

/** Minimal state accessor the processor uses instead of directly touching Spark ValueState. */
trait HeatStateStore {
  def get(): HeatState
  def update(s: HeatState): Unit
}
