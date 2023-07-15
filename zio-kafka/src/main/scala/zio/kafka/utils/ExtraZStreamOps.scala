package zio.kafka.utils

import zio._
import zio.stream._

object ExtraZStreamOps {

  // noinspection SimplifyWhenInspection
  implicit class ZStreamOps[R, E, A](val stream: ZStream[R, E, A]) extends AnyVal {

    type Interruptor[E1 >: E] = Promise[E1, Unit]
    type StartTimer           = UIO[Unit]
    type ResetTimer           = UIO[Unit]

    private def timer[E1 >: E](
      e: => E1
    )(after: Duration): URIO[Scope, (Interruptor[E1], StartTimer, ResetTimer)] =
      for {
        scope                  <- ZIO.scope
        now                    <- Clock.instant
        lastChunkReceivedAtRef <- Ref.Synchronized.make(now)
        tickRef                <- Ref.Synchronized.make[Option[Fiber[Nothing, Any]]](Option.empty)
        started                <- Ref.Synchronized.make(false)
        p                      <- Promise.make[E1, Unit]
        afterAsMillis = after.toMillis
        failPromiseIfNeeded = (reason: String) =>
                                lastChunkReceivedAtRef.updateZIO { lastChunkReceivedAt =>
                                  Clock.instant.flatMap { now =>
                                    val deadline         = lastChunkReceivedAt.plusMillis(afterAsMillis)
                                    val deadlineIsPassed = deadline.isBefore(now)

                                    if (deadlineIsPassed)
                                      ZIO.debug(s"Interrupted at $now from $reason") *>
                                        (
                                          p.fail(e) *>
                                            tickRef.updateSomeZIO { case Some(fiber) =>
                                              ZIO.debug(s"Interrupt TICK at $now from $reason") *>
                                                fiber.interrupt.as(None)
                                            }
                                        ).as(lastChunkReceivedAt)
                                    else
                                      ZIO.debug(s"Not interrupted at $now from $reason") *>
                                        ZIO.succeed(now)
                                  }
                                }
        startBackgroundInterruptorIfNot = started.updateSomeZIO { case false =>
                                            Clock.instant.flatMap(now =>
                                              ZIO.debug(s"Starting background interruptor $now")
                                            ) *>
                                              failPromiseIfNeeded("tick")
                                                .repeat(Schedule.fixed(after))
                                                .forkIn(scope)
                                                .flatMap(fiber => tickRef.set(Some(fiber)))
                                                .as(true)
                                          }
        resetTimer = failPromiseIfNeeded("reset")
      } yield (p, startBackgroundInterruptorIfNot, resetTimer)

    /**
     * Fails the stream with given error if it is not consumed (pulled) from, for some duration.
     *
     * Also see [[zio.stream.ZStream#timeoutFail]] for failing the stream doesn't _produce_ a value.
     */
    def consumeTimeoutFail[E1 >: E](e: => E1)(after: Duration): ZStream[R, E1, A] =
      ZStream.unwrapScoped {
        for {
          (p, startTimer, resetTimer) <- timer(e)(after)
        } yield stream
          .interruptWhen(p)
          .mapChunksZIO(data => ZIO.debug("Data received") *> startTimer *> resetTimer.as(data))
      }
  }

}
