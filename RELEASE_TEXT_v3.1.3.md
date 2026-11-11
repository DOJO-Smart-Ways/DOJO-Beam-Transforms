# Release v3.1.3

Patch for BigQuery → Parquet DATETIME compatibility with Apache Beam 2.72.0.

- Normalize DATETIME fields identified by the existing table schema before WriteToParquet.
- Accept strings with space or T separators and up to six fractional digits; preserve naive datetime objects, None and other fields.
- Reject invalid or timezone-bearing values with a field-specific error without record contents.
- Preserve the public API, BigQuery read mode, output paths and truncate behavior.
- No dependency upgrades.

Validation: 23 utils tests passed, including Arrow table construction and real local Parquet write/read through Beam, without GCP.

## Consume the Git release

In requirements.txt:

```text
dojo-beam-transforms @ git+https://github.com/DOJO-Smart-Ways/DOJO-Beam-Transforms.git@release-v3.1.3
```

Replace the previous dependency reference, install it in the submitting environment,
and rebuild the consumer's Dataflow image using the same tag. For the Dockerfile
in this repository:

```bash
docker build --no-cache -t REGION-docker.pkg.dev/PROJECT_ID/dojo-beam/dojo_beam:3.1.3 .
```

After publishing the image to the consumer's registry, use that image as
sdk_container_image with sdk_location=container. Rebuild any stored template
that embeds the old image reference before running the next job.

The Git tag makes this version installable from Git. It does not publish a PyPI
package or a container image. Those are separate publication steps.
