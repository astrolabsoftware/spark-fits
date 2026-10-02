# Releasing

The [CI workflow](../.github/workflows/ci.yml) runs fixture-backed tests for
Spark 2.4.8, 3.4.4, 3.5.9 (Scala 2.12.21) and Spark 4.2.0 (Scala 2.13.18)
on pull requests and pushes to `master`. The Spark 3.4.4 job also enforces the
70% coverage threshold. All four combinations run again at the release tag.

An upstream maintainer with publishing access to the existing
`com.github.astrolabsoftware` namespace in the [Central Portal](https://central.sonatype.org/register/central-portal/)
must create a Portal user token and configure these secrets for the
`maven-central` GitHub Actions environment:

- `SONATYPE_USERNAME` and `SONATYPE_PASSWORD`: Portal user-token credentials.
- `PGP_SECRET`: base64-encoded export of the GPG private signing key.
- `PGP_PASSPHRASE`: passphrase for that key.

The public GPG key must be available on a keyserver. Restrict access to the
`maven-central` environment to trusted maintainers and the `master` branch.
No publishing credentials are needed for pull-request CI.

After CI passes on `master`, create and push a tag such as `v1.1.0` on that
commit. From the upstream repository's **Actions → Release → Run workflow**,
enter the existing tag. The workflow verifies that it points to a commit on
`master`, derives `1.1.0` from the tag, tests all four combinations, then
signs and stages the `_2.12` and `_2.13` artifacts in one bundle. sbt's
`sonaRelease` uploads the bundle to the Central Portal and publishes it after
validation. A GitHub Release is created only after publication succeeds.

The regular build defaults to `1.0.0-SNAPSHOT`; `RELEASE_VERSION` is set only
by the release workflow. `SPARK_VERSION` overrides the build's Spark version
for compatibility checks; by default Scala 2.12 compiles against Spark 3.4.4
and Scala 2.13 against Spark 4.2.0.
