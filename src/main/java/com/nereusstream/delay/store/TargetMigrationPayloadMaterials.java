package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetMigrationObjectReference;
import com.nereusstream.delay.protocol.TargetMigrationPayloadHistory;
import com.nereusstream.delay.protocol.TargetMigrationPrefixRecord;
import com.nereusstream.delay.protocol.TargetMigrationRecordReference;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.runtime.CommandResult;
import com.nereusstream.delay.runtime.PayloadReservation;
import com.nereusstream.delay.runtime.PayloadReservationStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetMigrationPayloadJoins;
import com.nereusstream.delay.runtime.TargetReservationRecord;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Original protected migration material, read in full outside Store locks before freezing or authenticating a reclaim. */
public record TargetMigrationPayloadMaterials(
        LegacyMigrationBundle.Material material,
        LegacyMigrationBundle.Held held,
        LegacyMigrationBundle.Limits limits,
        LegacyMigrationBundle.ImmutableObjects objects) {
    public TargetMigrationPayloadMaterials {
        Objects.requireNonNull(material);
        Objects.requireNonNull(held);
        Objects.requireNonNull(limits);
        Objects.requireNonNull(objects);
    }

    public TargetMigrationPayloadHistory collect(
            TargetCheckpointDependencies d, TargetScheduleBinding binding, TargetReservationRecord reservation) {
        final var records = readComplete(d);
        final var root = Objects.requireNonNull(d.migrationRoot());
        final var plan = root.plan();
        TargetMigrationPayloadJoins.require(
                TargetMigrationPayloadJoins.checkpoint(d),
                Objects.requireNonNull(d.payloadOwner(binding.messageId()), "actual payload owner"),
                binding,
                reservation,
                null,
                d.root().aggregate().mutation());
        final var selected = new ArrayList<TargetMigrationPayloadHistory.Evidence>();
        if (reservation == null) {
            selectRef(selected, records, TargetMigrationPayloadHistory.Kind.BINDING, binding.migrationRecord());
            final var command = selectSource(
                    selected,
                    records,
                    TargetMigrationPayloadHistory.Kind.SCHEDULE_SOURCE,
                    binding.bindingSource(),
                    null);
            selectRaw(
                    selected,
                    records,
                    TargetMigrationPayloadHistory.Kind.SCHEDULE_FIRST,
                    KeyCodec.dedupeCommand(command.commandId()));
            selectRaw(
                    selected,
                    records,
                    TargetMigrationPayloadHistory.Kind.SCHEDULE_RESULT,
                    KeyCodec.dedupeResult(command.commandId()));
        } else {
            final var origin = Objects.requireNonNull(reservation.migrationOrigin(), "migration reservation origin");
            origin.requirePlan(plan, binding);
            selectRef(selected, records, TargetMigrationPayloadHistory.Kind.RESERVATION, origin.originalReservation());
            selectRef(selected, records, TargetMigrationPayloadHistory.Kind.BINDING, binding.migrationRecord());
            selectRef(selected, records, TargetMigrationPayloadHistory.Kind.RECEIPT, origin.receiptRecord());
            selectSource(
                    selected,
                    records,
                    TargetMigrationPayloadHistory.Kind.PREPARE_SOURCE,
                    origin.originalPrepareSource(),
                    reservation.prepareCommandId());
            selectRaw(
                    selected,
                    records,
                    TargetMigrationPayloadHistory.Kind.PREPARE_FIRST,
                    KeyCodec.dedupeCommand(reservation.prepareCommandId()));
            selectRaw(
                    selected,
                    records,
                    TargetMigrationPayloadHistory.Kind.PREPARE_RESULT,
                    KeyCodec.dedupeResult(reservation.prepareCommandId()));
            if (origin.originalStatus() == PayloadReservationStatus.COMMITTED) {
                final var command = selectSource(
                        selected,
                        records,
                        TargetMigrationPayloadHistory.Kind.COMMIT_SOURCE,
                        origin.originalMutationSource(),
                        null);
                selectRaw(
                        selected,
                        records,
                        TargetMigrationPayloadHistory.Kind.COMMIT_FIRST,
                        KeyCodec.dedupeCommand(command.commandId()));
                selectRaw(
                        selected,
                        records,
                        TargetMigrationPayloadHistory.Kind.COMMIT_RESULT,
                        KeyCodec.dedupeResult(command.commandId()));
            }
        }
        final var bindingOrigin = d.migrationOrigins().stream()
                .filter(value -> value.newFamily() == ColumnFamily.ID
                        && Arrays.equals(value.newKeyDigest(), Bytes.sha256(binding.encodedKey())))
                .findFirst()
                .orElseThrow();
        final var history = new TargetMigrationPayloadHistory(root.digest(), bindingOrigin, selected);
        history.require(root, binding, reservation);
        if (reservation != null) {
            final var origin = reservation.migrationOrigin();
            final var old = PayloadReservation.decode(ValueEnvelope.decode(
                            TargetMigrationPayloadHistory.raw(
                                            history.get(TargetMigrationPayloadHistory.Kind.RESERVATION))
                                    .value(),
                            2)
                    .payload());
            held.requireReservationCustody(old, origin.originalReceipt());
        }
        held.requireCurrent();
        return history;
    }
    /** Full original image/bundle authentication also for a migration with no payload rows. */
    public void requireComplete(TargetCheckpointDependencies dependencies) {
        readComplete(dependencies);
    }

    private List<TargetMigrationPayloadHistory.Evidence> readComplete(TargetCheckpointDependencies d) {
        final var root = Objects.requireNonNull(d.migrationRoot(), "actual migration A");
        final var plan = root.plan();
        held.requireCurrent();
        TargetMigrationPayloadJoins.requireActivation(
                TargetMigrationPayloadJoins.checkpoint(d),
                root,
                d.root().aggregate().mutation());
        plan.oldManifest().requireContent(material.view().manifest().canonicalJsonBytes());
        if (!material.summary().equals("READY")
                || !material.baseline().equals(plan.baseline())
                || !Arrays.equals(
                        material.view().manifest().manifestSha256(),
                        plan.oldManifest().resource().manifestSha256())
                || !Arrays.equals(material.view().manifest().recoveryLineageId(), plan.oldLineage())) {
            throw new IllegalStateException("payload history changed actual original complete C image");
        }
        held.requireClosedWriterDomain(material.view(), material.mappings());
        held.requireLegacyCustody(material.view(), material.externalRecords());
        held.requireConservation(material);
        final var mapping = LegacyMigrationBundle.readCompanion(
                plan.mapping().resource(), LegacyMigrationBundle.Section.MAPPING, material, held, limits, objects);
        final var custody = LegacyMigrationBundle.readCompanion(
                plan.custody().resource(), LegacyMigrationBundle.Section.CUSTODY, material, held, limits, objects);
        final var conservation = LegacyMigrationBundle.readCompanion(
                plan.conservation().resource(),
                LegacyMigrationBundle.Section.CONSERVATION,
                material,
                held,
                limits,
                objects);
        if (mapping.records().size() != material.mappings().size()) {
            throw new IllegalStateException("migration payload mapping is incomplete");
        }
        for (int i = 0; i < material.mappings().size(); i++) {
            same(mapping.records().get(i), material.mappings().get(i).canonicalBytes());
        }
        if (conservation.records().size() != 1) {
            throw new IllegalStateException("migration payload conservation is incomplete");
        }
        same(
                conservation.records().getFirst(),
                Bytes.concat(
                        material.baseline().canonicalBytes(),
                        material.view().manifest().manifestSha256(),
                        material.conservation().canonicalBytes()));
        // Full C custody is checked against the genuinely captured complete image, never a caller's selected row list.
        final List<byte[]> expected = new ArrayList<>();
        for (var row : material.view().entries()) {
            expected.add(Bytes.concat(Bytes.u8(1), row.canonicalBytes()));
        }
        for (var row : material.dispositions()) {
            expected.add(Bytes.concat(Bytes.u8(2), row.canonicalBytes()));
        }
        for (var row : material.entities()) {
            expected.add(Bytes.concat(Bytes.u8(3), row.canonicalBytes()));
        }
        for (var row : material.externalRecords()) {
            expected.add(Bytes.concat(Bytes.u8(4), row.canonicalBytes()));
        }
        if (expected.size() != custody.records().size()) {
            throw new IllegalStateException("migration payload original universe is incomplete");
        }
        for (int i = 0; i < expected.size(); i++) {
            same(expected.get(i), custody.records().get(i));
        }
        final List<TargetMigrationPayloadHistory.Evidence> records = new ArrayList<>();
        addLoaded(records, custody);
        // Existing prefix inventories preserve actual original frames/after-rows. Their full closure was authenticated
        // with actual A by ControlImageAuthority; read their exact complete fixed inventories again for these facts.
        for (var prefix : d.migrationPrefixes()) {
            prefix.requirePlan(plan);
            final var loaded = LegacyMigrationBundle.readCompanion(
                    prefix.sourceRecord().object().resource(),
                    LegacyMigrationBundle.Section.CUSTODY,
                    material,
                    held,
                    limits,
                    objects);
            final var projection = LegacyMigrationBundle.readCompanion(
                    prefix.projection().resource(),
                    LegacyMigrationBundle.Section.CUSTODY,
                    material,
                    held,
                    limits,
                    objects);
            requirePrefix(prefix, loaded, projection);
            addLoaded(records, loaded);
        }
        for (var origin : d.migrationOrigins()) {
            origin.requirePlan(plan);
            final var found = records.stream()
                    .filter(value -> value.reference().equals(origin.oldRecord()))
                    .findFirst()
                    .orElseThrow(() ->
                            new IllegalStateException("Native66 original row is outside complete sealed custody"));
            final var raw = TargetMigrationPayloadHistory.raw(found);
            if (raw.family() != origin.oldFamily()
                    || !Arrays.equals(Bytes.sha256(raw.key()), origin.oldKeyDigest())
                    || !Arrays.equals(Bytes.sha256(raw.value()), origin.oldValueDigest())) {
                throw new IllegalStateException("Native66 changed complete original raw family/key/value");
            }
        }
        held.requireCurrent();
        return records;
    }

    private static void requirePrefix(
            TargetMigrationPrefixRecord prefix,
            LegacyMigrationBundle.LoadedCompanion source,
            LegacyMigrationBundle.LoadedCompanion projection) {
        if (prefix.sourceRecord().ordinal() != 0
                || source.records().isEmpty()
                || projection.records().isEmpty()) {
            throw new IllegalStateException("migration prefix lost complete original Source/projection closure");
        }
        final var in = java.nio.ByteBuffer.wrap(source.records().getFirst());
        if (!in.hasRemaining() || in.get() != 1) {
            throw new IllegalArgumentException("unknown original migration prefix frame record");
        }
        final byte[] prior = lp(in), at = lp(in);
        if (in.remaining() < 8) {
            throw new IllegalArgumentException("truncated original prefix sequence");
        }
        in.getLong();
        final byte[] frame = lp(in), outcome = lp(in);
        if (in.hasRemaining()
                || !Arrays.equals(prior, prefix.prior().source().canonicalBytes())
                || !Arrays.equals(at, prefix.at().source().canonicalBytes())
                || !Arrays.equals(Bytes.sha256(frame), prefix.rawFrameDigest())) {
            throw new IllegalStateException("migration prefix changed actual original predecessor/Source/frame");
        }
        if (prefix.recordKind() == ProtocolTuple.CLIENT_COMMAND) {
            final var command = CommandCodec.decodeFrame(frame);
            CommandResult.decode(outcome);
            if (!Arrays.equals(command.commandId().bytes(), prefix.originalIdentity())
                    || !Arrays.equals(command.commandHash(), prefix.originalSemanticHash())) {
                throw new IllegalStateException("migration prefix changed original Command identity");
            }
        } else {
            final var mutation = SystemMutation.decodeEnvelope(frame);
            SystemMutationResult.decode(outcome);
            if (!Arrays.equals(mutation.systemMutationId(), prefix.originalIdentity())
                    || !Arrays.equals(mutation.mutationHash(), prefix.originalSemanticHash())) {
                throw new IllegalStateException("migration prefix changed original System identity");
            }
        }
        final byte[] header = projection.records().getFirst();
        final byte[] expected = Bytes.concat(Bytes.u8(1), at, prefix.rawFrameDigest());
        if (!Arrays.equals(header, expected)
                && !(projection.records().size() == 1
                        && Arrays.equals(header, Bytes.concat(expected, Bytes.u32be(0))))) {
            throw new IllegalStateException("migration projection changed original actual Source/frame closure");
        }
    }

    private static byte[] lp(java.nio.ByteBuffer in) {
        if (in.remaining() < 4) {
            throw new IllegalArgumentException("truncated original prefix length");
        }
        final int n = in.getInt();
        if (n < 0 || n > in.remaining()) {
            throw new IllegalArgumentException("invalid original prefix length");
        }
        final byte[] value = new byte[n];
        in.get(value);
        return value;
    }

    public void requireActual(
            TargetCheckpointDependencies d,
            TargetScheduleBinding binding,
            TargetReservationRecord reservation,
            TargetMigrationPayloadHistory frozen) {
        same(
                Objects.requireNonNull(frozen).canonicalBytes(),
                collect(d, binding, reservation).canonicalBytes());
    }

    private static void addLoaded(
            List<TargetMigrationPayloadHistory.Evidence> values, LegacyMigrationBundle.LoadedCompanion loaded) {
        final var rows = loaded.records();
        for (int i = 0; i < rows.size(); i++) {
            final var location = loaded.companion().locate(i);
            values.add(new TargetMigrationPayloadHistory.Evidence(
                    TargetMigrationPayloadHistory.Kind.RESERVATION,
                    new TargetMigrationRecordReference(
                            new TargetMigrationObjectReference(location.object()), location.ordinal()),
                    rows.get(i)));
        }
    }

    private static void selectRef(
            List<TargetMigrationPayloadHistory.Evidence> selected,
            List<TargetMigrationPayloadHistory.Evidence> records,
            TargetMigrationPayloadHistory.Kind kind,
            TargetMigrationRecordReference ref) {
        final var actual = records.stream()
                .filter(value -> value.reference().equals(ref))
                .findFirst()
                .orElseThrow(
                        () -> new IllegalStateException("original migration record outside complete fixed custody"));
        selected.add(new TargetMigrationPayloadHistory.Evidence(kind, actual.reference(), actual.record()));
    }

    private static void selectRaw(
            List<TargetMigrationPayloadHistory.Evidence> selected,
            List<TargetMigrationPayloadHistory.Evidence> records,
            TargetMigrationPayloadHistory.Kind kind,
            byte[] key) {
        TargetMigrationPayloadHistory.Evidence actual = null;
        for (var record : records) {
            final LegacyCheckpointStateInventory.RawEntry row;
            try {
                row = TargetMigrationPayloadHistory.raw(record);
            } catch (IllegalArgumentException failure) {
                continue;
            }
            if (row.family() == ColumnFamily.DEDUPE && Arrays.equals(row.key(), key)) {
                if (actual != null
                        && !Arrays.equals(
                                TargetMigrationPayloadHistory.raw(actual).value(), row.value())) {
                    throw new IllegalStateException("original first changed across migration custody");
                }
                if (actual == null) {
                    actual = record;
                }
            }
        }
        if (actual == null) {
            throw new IllegalStateException("complete migration custody lost original first/result");
        }
        selected.add(new TargetMigrationPayloadHistory.Evidence(kind, actual.reference(), actual.record()));
    }

    private static PreparedCommand selectSource(
            List<TargetMigrationPayloadHistory.Evidence> selected,
            List<TargetMigrationPayloadHistory.Evidence> records,
            TargetMigrationPayloadHistory.Kind kind,
            SourcePosition source,
            CommandId id) {
        for (var record : records) {
            final LegacyMigrationBundle.ExternalRecord external;
            try {
                external = TargetMigrationPayloadHistory.external(record);
            } catch (IllegalArgumentException failure) {
                continue;
            }
            if (!external.kind().equals("SOURCE")
                    || !Arrays.equals(external.source().canonicalBytes(), source.canonicalBytes())) {
                continue;
            }
            final var command = CommandCodec.decodeFrame(external.originalBytes());
            if (id != null && !id.equals(command.commandId())) {
                continue;
            }
            selected.add(new TargetMigrationPayloadHistory.Evidence(kind, record.reference(), record.record()));
            return command;
        }
        throw new IllegalStateException("complete migration custody lost actual original Source Command");
    }

    private static void same(byte[] a, byte[] b) {
        if (!Arrays.equals(a, b)) {
            throw new IllegalStateException("complete migration payload material changed exact immutable bytes");
        }
    }
}
