import yaml
import sys
from typing import List, Dict, Any, Union
from pydantic import BaseModel
from sardana.macroserver.msexception import UnknownEnv

ALLOWED_PREFIX = ["sarenv://", "mgdata://", "snapshot://"]  # "file://"]


class ExperimentInfo(BaseModel):
    exp_id: str
    exp_desc: str
    proposal_id: int
    exp_team: List[Dict[str, str]]
    safety_info: str


class NXinstrument(BaseModel):
    name: str
    klass: str
    other_fields: Dict[str, Any]


class NexusApplicationDefinition(BaseModel):
    definition: str
    beamline: str
    facility: str
    instruments: List[NXinstrument]
    experiment_info: Union[str, dict]
    macros: List[str]

    def getExperimentInfo(self, macro=None) -> ExperimentInfo:
        # check if experiment_info is a dict or string
        if isinstance(self.experiment_info, dict):
            return ExperimentInfo(**self.experiment_info)
        elif isinstance(self.experiment_info, str):
            if macro is None:
                raise Exception(
                    "macro is None, cannot read experiment info from env")
            # check if experiment_info is a reference to a sardana env var
            try:
                scheme, path = split_sarurl(self.experiment_info)
            except ValueError:
                raise ValueError(
                    "experiment_info should be a reference to sardana env var as sarenv://VAR")
            if "sarenv" in scheme:
                try:
                    expinfo = macro.getMacroServer().get_env(path)
                    return ExperimentInfo(**expinfo)
                except UnknownEnv as e:
                    macro.error("No {} environment variable found".format(
                        self.app_definition_env_name))
                    raise e
            else:
                raise ValueError(
                    "experiment_info should be a reference to sardana env var as sarenv://VAR")
        else:
            raise ValueError(
                "experiment_info must be a dict or a reference to a sardana environment variable")


def is_sarurl(sarurl):
    if any(sarurl.startswith(prefix) for prefix in ALLOWED_PREFIX):
        return True
    return False


def is_sarenv(sarurl):
    return "sarenv" in sarurl


def is_mgdata(sarurl):
    return "mgdata" in sarurl


def is_snapshot(sarurl):
    return "snapshot" in sarurl


def split_sarurl(sarurl):
    try:
        scheme, path = sarurl.split("://", 1)
        return scheme, path
    except ValueError:
        raise ValueError(
            "{} is not a valid url format (PREFIX://PATH)".format(sarurl))


def get_sarenv(sarurl, macro=None):
    scheme, path = split_sarurl(sarurl, 1)
    if "sarenv" in scheme:
        if macro is None:
            raise Exception(
                "macro object not provided, cannot get environment data")
        try:
            env_value = macro.getMacroServer().get_env(path)
            return env_value
        except UnknownEnv as e:
            macro.error("No {} environment variable found".format(path))
            raise e
    else:
        raise ValueError(
            "{} is not a valid reference to sardana env var (sarenv://VAR)".format(sarurl))


def search_channel(instrument: NXinstrument, channel_name: str):
    for ch in instrument.other_fields.items():
        if ch.name == channel_name:
            return ch.source
    return None


def parse_instruments(instruments_data):
    instruments = []
    for k, v in instruments_data.items():
        klass = v.pop('klass')
        instruments.append(NXinstrument(name=k, klass=klass, other_fields=v))
    return instruments


def parse_yaml(yaml_file_path) -> NexusApplicationDefinition:
    parsed_dict = None
    try:
        with open(yaml_file_path, 'r') as file:
            parsed_dict = yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"YAML file {yaml_file_path} not found")
    except Exception as e:
        raise e(
            f"Error parsing YAML file {yaml_file_path}")

    # Parse the YAML data and create NexusApplicationDefinition object
    nxdef = NexusApplicationDefinition(
        definition=parsed_dict['definition'],
        beamline=parsed_dict['beamline'],
        facility=parsed_dict['facility'],
        instruments=parse_instruments(parsed_dict['instrument']),
        experiment_info=parsed_dict['experiment_info'],
        macros=parsed_dict['macros']
    )
    return nxdef


if __name__ == '__main__':
    nxad = parse_yaml(sys.argv[1])
    model_dict = nxad.dict()
    print(model_dict)
    nxad.getExperimentInfo()
