// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Freeze a durable metadata cut on the SQL owner, then encode its immutable
//! pin on a separately admitted blocking owner. No partially encoded descriptor
//! escapes and no captured bytes supply publication or membership authority.

use super::*;

pub(crate) struct SnapshotCapture {
    group: GroupLog,
    descriptor: Descriptor,
    pin: Option<GenerationPin>,
    limits: SnapshotLimits,
    expected_payload: Option<(usize, [u8; 32])>,
}

impl SnapshotCapture {
    pub(crate) fn retained(
        group: &GroupLog,
        images: &GenerationStore,
        schema: [u8; 32],
        expected: &Snapshot,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        let snapshot = group.read()?.snapshot.clone();
        if snapshot != *expected {
            return Err(SnapshotError::Invalid("requested checkpoint replaced"));
        }
        let descriptor = Descriptor::decode(group, schema, &snapshot, limits)?;
        // A resident retained image is already independently verified. If a
        // caller has only an inventory entry, adopt still performs the complete
        // physical proof; unknown files never become trusted metadata pins.
        let pin = descriptor
            .image
            .as_ref()
            .map(|image| {
                images
                    .adopt(marker_id(image)?, marker_state(image)?)
                    .map_err(SnapshotError::Images)
            })
            .transpose()?;
        let expected_payload = Some((descriptor.encoded_length, descriptor.encoded_digest));
        Ok(Self {
            group: group.clone(),
            descriptor,
            pin,
            limits,
            expected_payload,
        })
    }

    pub(crate) fn capture(
        group: &GroupLog,
        image: PublishedImage,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        let schema = image.application_digest();
        Self::capture_at(group, Some(image), schema, limits)
    }

    pub(crate) fn capture_empty(
        group: &GroupLog,
        schema: [u8; 32],
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        Self::capture_at(group, None, schema, limits)
    }

    fn capture_at(
        group: &GroupLog,
        image: Option<PublishedImage>,
        schema: [u8; 32],
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        limits.validate()?;
        let pin = image.as_ref().map(PublishedImage::pin);
        let (position, conf, membership) = {
            let cache = group.read()?;
            if cache.applied.image != pin.as_ref().map(marker)
                || image.as_ref().is_some_and(|image| {
                    image.scope() != &group.identity().scope
                        || image.position().index > cache.applied.position.index
                        || cache.term(image.position().index.get())
                            != Some(image.position().term.get())
                })
                || cache.applied.position.index.get() > cache.hard_state.commit
            {
                return Err(SnapshotError::Invalid("source durable cut"));
            }
            (
                cache.applied.position,
                manifest::encode_conf(&cache.conf_state)
                    .map_err(|_| SnapshotError::Invalid("source configuration"))?,
                cache.membership.clone(),
            )
        };
        let descriptor = Descriptor {
            format: 1,
            scope: WireScope::from(&group.identity().scope),
            birth: group.birth_digest,
            schema,
            index: position.index.get(),
            term: position.term.get(),
            conf,
            membership,
            image: pin.as_ref().map(marker),
            encoded_length: 0,
            encoded_digest: Sha256::digest([]).into(),
        };
        Ok(Self {
            group: group.clone(),
            descriptor,
            pin,
            limits,
            expected_payload: None,
        })
    }

    pub(crate) fn encode(self) -> Result<SnapshotPackage, SnapshotError> {
        self.encode_with_io_profile(stream_snapshot::IoProfile::Compact)
    }

    /// The host's source reservation covers this profile through physical
    /// encoding and all consumers. Standalone capture retains Compact defaults.
    pub(crate) fn encode_with_io_profile(
        self,
        profile: stream_snapshot::IoProfile,
    ) -> Result<SnapshotPackage, SnapshotError> {
        let bytes = self
            .pin
            .as_ref()
            .map(|pin| encode_image(pin, self.limits, profile))
            .transpose()?;
        self.finish(bytes.map(SourcePayload::Local))
    }

    pub(crate) fn encode_prepared(
        self,
        cache: &prepared::Cache,
        profile: stream_snapshot::IoProfile,
    ) -> Result<(SnapshotPackage, bool), SnapshotError> {
        let Some(pin) = self.pin.as_ref() else {
            return Ok((self.finish(None)?, false));
        };
        let key = prepared::Key::new(
            &self.group,
            self.descriptor.schema,
            marker(pin),
            self.limits,
        )?;
        let pin = pin.clone();
        let limits = self.limits;
        let result = cache.prepare(key, move || encode_image(&pin, limits, profile))?;
        let reused = result.reused;
        Ok((
            self.finish(Some(SourcePayload::Shared(result.payload)))?,
            reused,
        ))
    }

    fn finish(mut self, bytes: Option<SourcePayload>) -> Result<SnapshotPackage, SnapshotError> {
        let identity = bytes.as_ref().map_or_else(
            || (0, Sha256::digest([]).into()),
            |body| {
                (
                    body.metadata().encoded_length as usize,
                    body.metadata().encoded_digest,
                )
            },
        );
        if self
            .expected_payload
            .is_some_and(|expected| expected != identity)
        {
            if let Some(body) = &bytes {
                body.invalidate();
            }
            return Err(SnapshotError::Invalid(
                "retained checkpoint payload binding",
            ));
        }
        self.descriptor.encoded_length = identity.0;
        self.descriptor.encoded_digest = identity.1;
        let snapshot = self.descriptor.snapshot()?;
        Descriptor::decode(&self.group, self.descriptor.schema, &snapshot, self.limits)?;
        Ok(SnapshotPackage {
            group: self.group,
            snapshot,
            bytes,
            pin: self.pin,
            limits: self.limits,
            membership: self.descriptor.membership,
        })
    }
}
