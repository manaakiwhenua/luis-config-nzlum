Configuration files for [LUIS](https://github.com/manaakiwhenua/luis) for the [NZLUM classification](https://github.com/manaakiwhenua/nzsluc/tree/main/classification-systems/nzlum)

The configuration file drives the workflow for producing land-use data.

Outputs are defined in configuration, and refer to inputs which are also defined in configuration. Outputs are affected by top-level configuration options, such as the DGGS resolution.

Configurations are written as YAML files. They are tracked in this repository for inclusion in LUIS as a git submodule.

JSON schema validation is performed by Snakemake on any included configuration, see `importer/src/config.schema.yml` in the LUIS repository for this schema. This explains what all the possible configuration options are, and **your workflow will not run if your configuration is not valid** according to this schema.

Configurations are split into separate files for clarity, but what is important is that **configurations are made to be combined into one configuration object**. Snakemake will merge multiple configuration files _in order_ with the `--configfiles` commandline options.

In Python, the YAML configurations are converted into dictionaries by Snakemake. Dictionaries are merged in the order specified by `--configfiles`. The intention is for configuration files to be specified from the most general to the most specific. For example, a run can be made that simply overwrites the H3 resolution, or region of interest, leaving the original or default configuration untouched.

## Division

The classification for LUIS is broken into two primary pieces:

1. Classifications
2. External data

Classifications are "output" land use classifications. Each land use classification has one or more input datasets, that's the "external data".

In addition to this, there are some other divisions of configuration. This division is rather about extending base configuration for particualar projects, or replacing parts of the configuration based on the intended environment (such as an HPC).

- profiles
    - for storing Snakemake run time configuration according to environment or project
- scopes
    - for extending configuration (or including private information) for a particular project, or environment
    - examples:
        - NeSI (used when running on the NeSI HPC)