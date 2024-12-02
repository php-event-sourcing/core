<?php

declare(strict_types=1);

use DbalEs\Dbal\Pdo\PdoConnection;
use DbalEs\Event;
use DbalEs\EventStreamId;
use DbalEs\Postgres\PostgresEventStore;
use DbalEs\Postgres\PostgresPersistentSubscriptions;
use DbalEs\Postgres\PostgresProjectionManager;
use DbalEs\Subscription\PersistentSubscriptions;
use DbalEsTests\Fixtures\InMemoryEventCounterProjector;
use DbalEsTests\Fixtures\PostgresEventCounterProjector;
use DbalEsTests\Fixtures\PostgresTableProjector;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\QuestionHelper;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Question\Question;
use Symfony\Component\Uid\Uuid;

require_once dirname(__DIR__) . '/vendor/autoload.php';

$application = new Application();

function createEventStore(): PostgresEventStore
{
    $pdo = new \PDO('pgsql:host=localhost;port=25432;dbname=app;user=app;password=!ChangeMe!');
    $connection = new PdoConnection($pdo);
    $postgresProjectionManager = new PostgresProjectionManager(
        connection: $connection,
        projectors: [
            'base' => new PostgresTableProjector($connection, 'test_event_base'),
            'catchup' => new PostgresTableProjector($connection, 'test_event_catchup'),
        ],
        ignoreUnknownProjectors: true);


    return new PostgresEventStore($connection, $postgresProjectionManager);
}

function createPersistentSubscriptions(PostgresEventStore $eventStore): PersistentSubscriptions
{
    return new PostgresPersistentSubscriptions($eventStore->connection(), $eventStore);
}

$application->register('long-running-append')
    ->addOption('streamId', null, InputOption::VALUE_OPTIONAL, 'Stream ID')
    ->addOption('start_event_count', null, InputOption::VALUE_OPTIONAL, 'Number of events appended at start', 1)
    ->addOption('event_count', null, InputOption::VALUE_OPTIONAL, 'Number of events to append', 1)
    ->setCode(function (InputInterface $input, OutputInterface $output) {
        $eventStore = createEventStore();
        $connection = $eventStore->connection();

        $streamId = new EventStreamId($input->getOption('streamId') ?: Uuid::v4());
        $startEventCount = (int) $input->getOption('start_event_count');
        $eventCount = (int) $input->getOption('event_count');

        $eventStore->append($streamId, array_map(fn () => new Event('start_event', ['data' => 'value']), range(1, $startEventCount)));

        $connection->beginTransaction();

        try {
            $eventStore->append($streamId, array_map(fn () => new Event('long_running_event', ['data' => 'value']), range(1, $eventCount)));

            $output->writeln('Events appended, waiting some input to commit');

            $questionHelper = new QuestionHelper();
            $question = new Question('Press enter to commit');

            $questionHelper->ask($input, $output, $question);

            if (! $connection->commit()) {
                $output->writeln('Commit failed');
                return Command::FAILURE;
            } else {
                $output->writeln('Events committed');
                return Command::SUCCESS;
            }
        } catch (\Throwable $e) {
            $output->writeln('Error occurred, rolling back');
            $connection->rollBack();
            throw $e;
        }
    });

$application->register("catchup-projection")
    ->addOption('streamId', null, InputOption::VALUE_OPTIONAL, 'Stream ID')
    ->setCode(function (InputInterface $input, OutputInterface $output) {
        $eventStore = createEventStore();
        $projectionManager = $eventStore->projectionManager();

        $output->writeln('Running catchup projection');

        try {
            $projectionManager->catchupProjection('catchup', createPersistentSubscriptions($eventStore));
        } catch (\Throwable $e) {
            $output->writeln(sprintf('Error occurred: %s', $e->getMessage()));
            throw $e;
        }

        $output->writeln('Catchup projection done');
        return Command::SUCCESS;
    });

$application->register("init")
    ->addOption('streamId', null, InputOption::VALUE_OPTIONAL, 'Stream ID')
    ->setCode(function (InputInterface $input, OutputInterface $output) {
        $eventStore = createEventStore();
        $projectionManager = $eventStore->projectionManager();

        $projectionManager->removeProjection('base');
        $projectionManager->removeProjection('catchup');

        $projectionManager->addProjection('base');
        $projectionManager->catchupProjection('base', createPersistentSubscriptions($eventStore));

        $projectionManager->addProjection('catchup');

        $output->writeln('Projections initialized');
        return Command::SUCCESS;
    });


$application->run();