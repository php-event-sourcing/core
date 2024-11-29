<?php

declare(strict_types=1);

namespace DbalEs;

/**
 * @template EventData
 */
readonly class Event
{
    /**
     * @param EventData $data
     */
    public function __construct(
        public string $type,
        public mixed $data
    ) {
    }
}