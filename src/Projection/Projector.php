<?php

declare(strict_types=1);

namespace DbalEs\Projection;

use DbalEs\PersistedEvent;

interface Projector
{
    public function project(PersistedEvent $event): void;
}