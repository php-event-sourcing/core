<?php

declare(strict_types=1);

namespace DbalEsTests;

use DbalEs\Dbal\Pdo\PdoConnection;
use DbalEs\Event;
use DbalEs\EventStreamId;
use DbalEs\Postgres\PostgresEventStore;
use DbalEs\Postgres\PostgresProjectionManager;
use DbalEsTests\Fixtures\InMemoryEventCounterProjector;
use DbalEsTests\Fixtures\PostgresTableProjector;
use PHPUnit\Framework\Attributes\CoversNothing;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\InputStream;
use Symfony\Component\Process\Process;
use Symfony\Component\Uid\Uuid;

#[CoversNothing]
class CatchUpSyncProjectionTest extends TestCase
{
    public function test_catch_up_sync_projection(): void
    {
        $pdo = new \PDO('pgsql:host=localhost;port=25432;dbname=app;user=app;password=!ChangeMe!');
        $connection = new PdoConnection($pdo);
        $projectionName = Uuid::v4()->toString();
        $streamId = new EventStreamId(Uuid::v4());

        $postgresProjectionManager = new PostgresProjectionManager(
            connection: $connection,
            projectors: [
                $projectionName => $counterProjection = new InMemoryEventCounterProjector([$streamId->streamId]),
            ],
            ignoreUnknownProjectors: true);
        $postgresProjectionManager->addProjection($projectionName);

        $eventStore = new PostgresEventStore($connection, $postgresProjectionManager);


        $eventStore->append($streamId, [
            new Event('event_type', ['data' => 'value']),
            new Event('event_type', ['data' => 'value']),
            new Event('event_type', ['data' => 'value']),
            new Event('event_type', ['data' => 'value']),
            new Event('event_type', ['data' => 'value']),
            new Event('event_type', ['data' => 'value']),
            new Event('event_type', ['data' => 'value']),
        ]);

        self::assertEquals(0, $counterProjection->getCounter());

        $postgresProjectionManager->catchupProjection($projectionName, $eventStore);

        self::assertEquals(7, $counterProjection->getCounter());

        $eventStore->append($streamId, [
            new Event('event_type', ['data' => 'value']),
            new Event('event_type', ['data' => 'value']),
        ]);

        self::assertEquals(9, $counterProjection->getCounter());

    }

    #[Test]
    public function with_multiple_parallel_processes(): void
    {
        $longRunningProcessInput = new InputStream();
        $longRunningProcess = new Process(['php', __DIR__ . '/process.php', 'long-running-append']);
        $longRunningProcess->setInput($longRunningProcessInput);
        $initProcess = new Process(['php', __DIR__ . '/process.php', 'init']);
        $catchupProcess = new Process(['php', __DIR__ . '/process.php', 'catchup-projection']);

        $pdo = new \PDO('pgsql:host=localhost;port=25432;dbname=app;user=app;password=!ChangeMe!');
        $connection = new PdoConnection($pdo);
        $counterBaseProjection = new PostgresTableProjector($connection, 'test_event_base');
        $counterCatchupProjection = new PostgresTableProjector($connection, 'test_event_catchup');

        $this->profileExecutionTime('Init process', function () use ($initProcess) {
            $initProcess->run();
        });
        self::assertEmpty($counterCatchupProjection->getState());

        $longRunningProcess->start();

        $this->profileExecutionTime('Wait for long running transaction', function () use ($longRunningProcess) {
            $longRunningProcess->waitUntil(function ($type, $output) {
                return $output === "Events appended, waiting some input to commit\n";
            });
        });

        $catchupProcess->start();

        $this->profileExecutionTime('Wait for catchup process', function () use ($counterBaseProjection, $counterCatchupProjection) {
            $maxSleep = 1000;
            while ($counterBaseProjection->getState() != $counterCatchupProjection->getState()) {
                usleep(1000);
                $maxSleep--;
                if ($maxSleep === 0) {
                    self::fail('Projection did not catch up');
                }
            }
        });

        $longRunningProcessInput->write("\n");
        $longRunningProcessInput->close();

        $this->profileExecutionTime('Wait for long running process', function () use ($longRunningProcess, $catchupProcess) {
            $longRunningProcess->wait();
            $catchupProcess->wait();
        });

        $realEventIdsStatement = $connection->prepare('SELECT id FROM es_event');
        $realEventIdsStatement->execute();
        $realEventIds = [];
        while ($row = $realEventIdsStatement->fetch()) {
            $realEventIds[] = (int) $row['id'];
        }

        self::assertTrue($longRunningProcess->isSuccessful());
        self::assertTrue($catchupProcess->isSuccessful(), $catchupProcess->getOutput());
        self::assertEquals($realEventIds, $counterBaseProjection->getState(), "Base projection did not catch up");
        self::assertEquals($realEventIds, $counterCatchupProjection->getState(), "Catchup projection did not catch up");
    }

    protected function profileExecutionTime(string $description, callable $callback): void
    {
        $startTime = microtime(true);
        $callback();
        $endTime = microtime(true);
//        echo $description . ' execution time: ' . ($endTime - $startTime) . ' seconds' . "\n";
    }
}