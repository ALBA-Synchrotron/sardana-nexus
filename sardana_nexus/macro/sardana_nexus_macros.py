import weakref
import copy
from sardana.macroserver.macro import macro, Type, Optional


NX_EXP_INFO_TEMPLATE = """\
Beamline: {beamline}
Experiment Identifier: {exp_id}
Experiment Description: {exp_desc}
Proposal ID: {proposal_id}
Session Name: {session}
Safety info: {safety_info}
Experimental team: {exp_team}
"""


class NexusExperimentInfo:
    env_name = 'NexusExperimentInfo'

    def __init__(self, macro):
        self.macro = weakref.proxy(macro)
        try:
            self._load_env()
        except Exception:
            self.macro.error('There is not %s environment.' % (self.env_name))
            self.macro.info('Creating....')
            self.nexus_info = {
                'beamline': 'beamline_id',
                'exp_id': 'identifier for the experiment',
                'exp_desc': 'description for the experiment',
                'proposal_id': 000000000,
                'exp_team': [],
                'safety_info': '',
                'session': "test_session",
            }
            self._save_env()

    def _load_env(self):
        self.nexus_info = copy.deepcopy(self.macro.getEnv(self.env_name))

    def _save_env(self):
        self.macro.setEnv(self.env_name, copy.deepcopy(self.nexus_info))

    def __repr__(self):
        pars = copy.deepcopy(self.nexus_info)
        return NX_EXP_INFO_TEMPLATE.format(**pars)

    @property
    def beamline(self):
        self._load_env()
        return self.nexus_info['beamline']

    @beamline.setter
    def beamline(self, bl):
        self._load_env()
        self.nexus_info['beamline'] = bl
        self._save_env()

    @property
    def exp_id(self):
        self._load_env()
        return self.nexus_info['exp_id']

    @exp_id.setter
    def exp_id(self, exp_id):
        self._load_env()
        self.nexus_info['exp_id'] = exp_id
        self._save_env()

    @property
    def exp_desc(self):
        self._load_env()
        return self.nexus_info['exp_desc']

    @exp_desc.setter
    def exp_desc(self, exp_desc):
        self._load_env()
        self.nexus_info['exp_desc'] = exp_desc
        self._save_env()

    @property
    def proposal_id(self):
        self._load_env()
        return self.nexus_info['proposal_id']

    @proposal_id.setter
    def proposal_id(self, proposal_id):
        self._load_env()
        self.nexus_info['proposal_id'] = proposal_id
        self._save_env()

    @property
    def exp_team(self):
        self._load_env()
        return self.nexus_info['exp_team']

    @exp_team.setter
    def exp_team(self, exp_team):
        self._load_env()
        self.nexus_info['exp_team'] = exp_team
        self._save_env()

    @property
    def safety_info(self):
        self._load_env()
        return self.nexus_info['safety_info']

    @safety_info.setter
    def safety_info(self, safety_info):
        self._load_env()
        self.nexus_info['safety_info'] = safety_info
        self._save_env()

    @property
    def session(self):
        self._load_env()
        return self.nexus_info['session']

    @session.setter
    def session(self, session_name):
        self._load_env()
        self.nexus_info['session'] = session_name
        self._save_env()


@macro()
def nexus_experiment_info(self):
    info = NexusExperimentInfo(self)
    self.info(info)


@macro([["beamline", Type.String, Optional, "Beamline indentifier"]])
def nexus_beamline(self, beamline):
    nxinfo = NexusExperimentInfo(self)
    if beamline is not None:
        nxinfo.beamline = beamline
    self.execMacro("nexus_experiment_info")


@macro([["proposalId", Type.Integer, Optional, "ProposalID"],
       ["experimentId", Type.String, Optional, "Experiment ID"],
       ["experimentDesc", Type.String, Optional, "Experiment Description"]])
def nexus_proposal_info(self, proposalId, experimentId, experimentDesc):
    nxinfo = NexusExperimentInfo(self)
    if proposalId is not None:
        nxinfo.proposal_id = proposalId
    if experimentId is not None:
        nxinfo.exp_id = experimentId
    if experimentDesc is not None:
        nxinfo.exp_desc = experimentDesc
    self.execMacro("nexus_experiment_info")


@macro([["session_name", Type.String, Optional, "Session name"]])
def nexus_session_name(self, session_name):
    nxinfo = NexusExperimentInfo(self)
    if session_name is not None:
        nxinfo.session = session_name
    self.execMacro("nexus_experiment_info")


@macro([["safetyinfo", Type.String, Optional, "Experiment ID"]])
def nexus_safety_info(self, safetyinfo):
    nxinfo = NexusExperimentInfo(self)
    if safetyinfo is not None:
        nxinfo.safety_info = safetyinfo
    self.execMacro("nexus_experiment_info")


@macro([["name", Type.String, None, "User Name"],
        ["email", Type.String, None, "User email"],
        ["role", Type.String, None,
            "User Role (main proposer, local contact,...)"],
        ["affiliation", Type.String, None, "Affiliation"],
        ["orcid", Type.String, Optional, "User ORCID ID"]])
def nexus_append_user(self, name, email, role, affiliation, orcid):
    nxinfo = NexusExperimentInfo(self)
    users = nxinfo.exp_team
    nxinfo.exp_team = users + [{'name': name, 'email': email,
                                'role': role, 'affiliation': affiliation, 'orcid': orcid}]
    self.execMacro("nexus_experiment_info")


@macro([["name", Type.String, None, "User Name"]])
def nexus_remove_user(self, name):
    nxinfo = NexusExperimentInfo(self)
    idx = -1
    for user in nxinfo.exp_team:
        if user["name"] == name:
            idx = nxinfo.exp_team.index(user)
            break
    if idx >= 0:
        users = nxinfo.exp_team
        users.pop(idx)
        nxinfo.exp_team = users
    self.execMacro("nexus_experiment_info")


@macro()
def nexus_clear_users(self):
    nxinfo = NexusExperimentInfo(self)
    nxinfo.exp_team = []
    self.execMacro("nexus_experiment_info")
