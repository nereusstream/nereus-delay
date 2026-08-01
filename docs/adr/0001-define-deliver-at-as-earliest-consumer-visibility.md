# Define `deliverAt` as earliest consumer visibility

Nereus Delay defines `deliverAt` as the earliest time a destination consumer may become eligible to receive a message, rather than when publishing starts or completes. This gives Kafka and Pulsar one business-level contract despite different delivery mechanics: Kafka strict publishing starts no earlier than `deliverAt` and may become visible later, while Pulsar may accept an earlier handoff but must keep the message invisible until `deliverAt`.
