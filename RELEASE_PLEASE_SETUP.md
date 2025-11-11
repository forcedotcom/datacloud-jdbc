# Release Please Implementation

## Overview

This project uses [Release Please](https://github.com/googleapis/release-please) to automate releases based on [Conventional Commits](https://www.conventionalcommits.org/).

## How It Works

### 1. Release PR Creation
- When conventional commits are merged to `main`, Release Please creates/updates a **release PR**
- The PR includes:
  - Version bump in `gradle.properties` (revision=X.Y.Z)
  - Updated `CHANGELOG.md` with all changes since last release
  - Updated `.release-please-manifest.json`

### 2. Release PR Merge
- When the release PR is merged, Release Please automatically:
  - Creates a Git tag (e.g., `0.40.0`)
  - Creates a GitHub release with changelog
  - Updates version files

### 3. Maven Central Publishing
- GitHub release triggers `release.yml` workflow
- Builds, tests, and publishes to Maven Central via `reusable-build-publish.yml`
- Updates GitHub release with Maven Central status

## Configuration Files

- **`release-please-config.json`** - Release Please configuration
- **`.release-please-manifest.json`** - Tracks current version
- **`CHANGELOG.md`** - Auto-generated changelog
- **`.github/workflows/release-please.yml`** - Release Please workflow

## Version Bumping

Release Please uses conventional commits to determine version bumps:

| Commit Type | Version Bump | Example |
|---|---|---|
| `feat:` | Minor | `0.39.0` → `0.40.0` |
| `fix:` | Patch | `0.39.0` → `0.39.1` |
| `feat!:` or `BREAKING CHANGE:` | Major | `1.0.0` → `2.0.0` |
| `docs:`, `chore:`, etc. | No release | (included in next release) |

## Workflow

```
1. Developer merges PR with conventional commit (e.g., "feat: add feature")
   ↓
2. Release Please creates/updates release PR
   ↓
3. Team reviews and merges release PR
   ↓
4. Release Please creates GitHub release
   ↓
5. release.yml publishes to Maven Central
   ↓
6. release-notifications.yml announces release
```

## Key Benefits

✅ **Automated changelog generation** from commit messages  
✅ **Version bump automation** based on conventional commits  
✅ **Release PR review** before publishing  
✅ **Consistent release process** with industry-standard tooling  

## References

- [Release Please Documentation](https://github.com/googleapis/release-please)
- [Release Please Action](https://github.com/marketplace/actions/release-please-action)
- [Conventional Commits](https://www.conventionalcommits.org/)
