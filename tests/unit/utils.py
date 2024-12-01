import os
from copy import deepcopy

from dbt.config import Profile
from dbt.config import Project
from dbt.config import RuntimeConfig
from dbt.config.project import PartialProject
from dbt.config.renderer import DbtProjectYamlRenderer
from dbt.config.renderer import ProfileRenderer
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
