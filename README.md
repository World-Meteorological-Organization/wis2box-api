# wis2box-api

WIS 2.0 in a box API provides [OGC API](https://ogcapi.ogc.org) support using [pygeoapi](https://pygeoapi.io), and intended for use for within the [wis2box](https://docs.wis2box.wis.wmo.int) project.

wis2box-api uses the base image provided by [dim_eccodes_baseimage](https://github.com/World-Meteorological-Organization/dim_eccodes_baseimage) to enable support for data conversion through pygeoapi processes.

## Installation

wis2box-api is part of the [wis2box](https://community.wmo.int/en/activity-areas/wis/wis2box) software stack.

### Dependencies

Dependencies are listed in [requirements.txt](requirements.txt). Dependencies are automatically installed during wis2box-api installation.

## Releasing

```bash
# create release (x.y.z is the release version)
vi wis2box_api/__init__.py  # update __version__
git commit -am 'update release version x.y.z'
git push origin main
git tag -a x.y.z -m 'tagging release version x.y.z'
git push --tags

# publish release on GitHub (https://github.com/World-Meteorological-Organization/wis2box-api/releases/new)

# bump version back to dev
vi wis2box_api/__init__.py  # update __version__
git commit -am 'back to dev'
git push origin main
```

### Code Conventions

* [PEP8](https://www.python.org/dev/peps/pep-0008)

## Issues

Please direct all issues to the [main wis2box issue tracker](https://github.com/World-Meteorological-Organization/wis2box/issues)

## Contact

* [Tom Kralidis](https://github.com/tomkralidis)
* [Maaike Limper](https://github.com/maaikelimper)
