<?php

declare(strict_types=1);

namespace DbalEs;

use Stringable;

readonly class EventStreamId
{
    public function __construct(
        public string|Stringable $streamId,
        public ?int    $version = null,
    ) {
    }

    public function withVersion(int $version): self
    {
        return new self($this->streamId, $version);
    }

    public function withoutVersion(): self
    {
        return new self($this->streamId);
    }

    public function equals(self $eventStreamId): bool
    {
        return $this->streamId === $eventStreamId->streamId && $this->version === $eventStreamId->version;
    }
}