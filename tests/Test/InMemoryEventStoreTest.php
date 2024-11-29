<?php

declare(strict_types=1);

namespace DbalEsTests\Test;

use DbalEs\EventStore;
use DbalEs\Test\InMemoryEventStore;
use DbalEsTests\EventStoreTestCase;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(InMemoryEventStore::class)]
class InMemoryEventStoreTest extends TestCase
{
    use EventStoreTestCase;
    
    protected function createEventStore(): EventStore
    {
        return new InMemoryEventStore();
    }
}
