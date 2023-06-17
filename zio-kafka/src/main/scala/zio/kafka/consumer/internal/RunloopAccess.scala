package zio.kafka.consumer.internal

import org.apache.kafka.common.TopicPartition
import zio.kafka.consumer.diagnostics.DiagnosticEvent.Finalization
import zio.kafka.consumer.diagnostics.Diagnostics
import zio.kafka.consumer.internal.Runloop.ByteArrayCommittableRecord
import zio.kafka.consumer.internal.RunloopAccess.PartitionAssignment
import zio.kafka.consumer.{ ConsumerSettings, Subscription }
import zio.stream.{ Stream, Take, UStream, ZStream }
import zio.{ Hub, Promise, Ref, Scope, UIO, ZIO, ZLayer }

// TODO: move to companion object of RunloopAccess
private[internal] sealed trait RunloopState
private[internal] object RunloopState {
  case object NotRunning                                       extends RunloopState
  final case class Started(runloop: Promise[Nothing, Runloop]) extends RunloopState
}

/**
 * [[RunloopAccess]] is the access point to the [[Runloop]]. It controls starting and stopping of the [[Runloop]]. This
 * can be used to make sure that the run loop is only running when there is at least one subscription active.
 */
private[consumer] final class RunloopAccess private (
  runloopStateRef: Ref.Synchronized[RunloopState],
  partitionHub: Hub[Take[Throwable, PartitionAssignment]],
  makeRunloop: UIO[Runloop],
  diagnostics: Diagnostics
) {

  private def runloop[A](requireRunning: Boolean, f: Runloop => UIO[A], notRunning: UIO[A]): UIO[A] = {
    def makeRunloopPromise: ZIO[Scope, Nothing, RunloopState.Started] = for {
      p <- Promise.make[Nothing, Runloop]
      _ <- makeRunloop.flatMap(p.succeed).forkScoped
    } yield RunloopState.Started(p)

    ZIO.scoped {
      runloopStateRef.updateSomeAndGetZIO {
        case RunloopState.NotRunning if requireRunning => makeRunloopPromise
      }.flatMap {
        case RunloopState.Started(p) => p.await.flatMap(f)
        case RunloopState.NotRunning => notRunning
      }
    }
  }

  private def withRunloopZIO[A](f: Runloop => UIO[A]): UIO[A] =
    runloop(requireRunning = true, f, ZIO.die(new IllegalStateException("Runloop did not start")))

  private def whenRunloopRunningZIO(f: Runloop => UIO[Unit]): UIO[Unit] =
    runloop(requireRunning = false, f, ZIO.unit)

  def stopConsumption(): UIO[Unit] = whenRunloopRunningZIO(_.stopConsumption)

  /**
   * We're doing all of these things in this method so that the interface of this class is as simple as possible and
   * there's no mistake possible for the caller.
   *
   * The external world (Consumer) doesn't need to know how we "subscribe", "unsubscribe", etc. internally.
   */
  def subscribe(
    subscription: Subscription
  ): ZIO[Scope, Throwable, UStream[Take[Throwable, PartitionAssignment]]] =
    for {
      stream <- ZStream.fromHubScoped(partitionHub)
      // starts the Runloop if not already started
      _ <- withRunloopZIO(_.addSubscription(subscription))
      _ <- ZIO.addFinalizer {
             whenRunloopRunningZIO(_.removeSubscription(subscription)) <*
               diagnostics.emit(Finalization.SubscriptionFinalized)
           }
    } yield stream

}

private[consumer] object RunloopAccess {
  type PartitionAssignment = (TopicPartition, Stream[Throwable, ByteArrayCommittableRecord])

  def make(
    settings: ConsumerSettings,
    diagnostics: Diagnostics = Diagnostics.NoOp,
    consumerAccess: ConsumerAccess,
    consumerSettings: ConsumerSettings
  ): ZIO[Scope, Throwable, RunloopAccess] =
    for {
      // This scope allows us to link the lifecycle of the Runloop and of the Hub to the lifecycle of the Consumer
      // When the Consumer is shutdown, the Runloop and the Hub will be shutdown too (before the consumer)
      consumerScope <- ZIO.scope
      partitionsHub <- ZIO
                         .acquireRelease(Hub.unbounded[Take[Throwable, PartitionAssignment]])(_.shutdown)
                         .provide(ZLayer.succeed(consumerScope))
      runloopStateRef <- Ref.Synchronized.make[RunloopState](RunloopState.NotRunning)
      makeRunloop = Runloop
                      .make(
                        hasGroupId = settings.hasGroupId,
                        consumer = consumerAccess,
                        pollTimeout = settings.pollTimeout,
                        diagnostics = diagnostics,
                        offsetRetrieval = settings.offsetRetrieval,
                        userRebalanceListener = settings.rebalanceListener,
                        restartStreamsOnRebalancing = settings.restartStreamOnRebalancing,
                        partitionsHub = partitionsHub,
                        consumerSettings = consumerSettings
                      )
                      .withFinalizer(_ => runloopStateRef.set(RunloopState.NotRunning))
                      .provide(ZLayer.succeed(consumerScope))
    } yield new RunloopAccess(runloopStateRef, partitionsHub, makeRunloop, diagnostics)
}
