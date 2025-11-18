import os
from copy import deepcopy
from typing import Optional

from dbt.adapters.databricks import catalogs, constants

from dbt.config import Profile, Project, RuntimeConfig
from dbt.config.project import PartialProject
from dbt.config.renderer import DbtProjectYamlRenderer, ProfileRenderer
from dbt.config.utils import parse_cli_vars


class Obj:
    which = "blah"
    single_threaded = False


def profile_from_dict(profile, profile_name, cli_vars="{}"):
    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = ProfileRenderer(cli_vars)

    # in order to call dbt's internal profile rendering, we need to set the
    # flags global. This is a bit of a hack, but it's the best way to do it.
    from argparse import Namespace

    from dbt.flags import set_from_args

    set_from_args(Namespace(), None)
    return Profile.from_raw_profile_info(
        profile,
        profile_name,
        renderer,
    )


def project_from_dict(project, profile, packages=None, selectors=None, cli_vars="{}"):
    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = DbtProjectYamlRenderer(profile, cli_vars)

    project_root = project.pop("project-root", os.getcwd())

    partial = PartialProject.from_dicts(
        project_root=project_root,
        project_dict=project,
        packages_dict=packages,
        selectors_dict=selectors,
    )
    return partial.render(renderer)


def config_from_parts_or_dicts(project, profile, packages=None, selectors=None, cli_vars="{}"):
    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    if isinstance(project, Project):
        profile_name = project.profile_name
    else:
        profile_name = project.get("profile")

    if not isinstance(profile, Profile):
        profile = profile_from_dict(
            deepcopy(profile),
            profile_name,
            cli_vars,
        )

    if not isinstance(project, Project):
        project = project_from_dict(
            deepcopy(project),
            profile,
            packages,
            selectors,
            cli_vars,
        )

    args = Obj()
    args.vars = cli_vars
    args.profile_dir = "/dev/null"
    return RuntimeConfig.from_parts(project=project, profile=profile, args=args)

def unity_relation(
    table_format: Optional[str] = None,
    file_format: Optional[str] = None,
    location_root: Optional[str] = None,
    location_path: Optional[str] = None,
) -> catalogs.DatabricksCatalogRelation:

    catalog_integration = constants.DEFAULT_UNITY_CATALOG

    return catalogs.DatabricksCatalogRelation(
        catalog_type=catalog_integration.catalog_type,
        catalog_name=catalog_integration.catalog_name,
        table_format=table_format or catalog_integration.table_format,
        file_format=file_format or catalog_integration.file_format,
        external_volume=location_root or catalog_integration.external_volume,
        location_path=location_path,
    )