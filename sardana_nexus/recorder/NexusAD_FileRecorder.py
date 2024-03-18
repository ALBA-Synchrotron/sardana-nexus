import re
import numpy
import posixpath
import h5py
from typing import Optional
from datetime import datetime
from sardana_nexus.utils.nexus_yaml import is_snapshot, parse_yaml, is_sarurl, split_sarurl, is_sarenv, is_mgdata
from sardana.macroserver.msexception import UnknownEnv
from sardana.macroserver.recorders.h5storage import NXscanH5_FileRecorder
from sardana.macroserver.scan.recorder import SaveModes
from sardana.sardanautils import is_pure_str


VDS_available = True
try:
    h5py.VirtualSource
except AttributeError:
    VDS_available = False


class NexusAD_FileRecorder(NXscanH5_FileRecorder):

    app_definition_env_name = "NexusApplicationDefinitionFile"

    def __init__(self, filename=None, macro=None, overwrite=False, **pars):
        NXscanH5_FileRecorder.__init__(
            self, filename=filename, macro=macro, overwrite=overwrite, **pars)

        # We read the YAML application definition
        try:
            yaml_file = self.macro().getMacroServer().get_env(self.app_definition_env_name)
            self.nxdef = parse_yaml(yaml_file)
        except UnknownEnv as e:
            self.macro().error("No {} environment variable found".format(
                self.app_definition_env_name))
            raise e
        except Exception as e:
            self.macro().error("Error reading Nexus Application Definition YAML")
            raise e

        # Other causes to disable the recording
        # Exceptions are raised so Sardana is not adding the recorder
        if self.nxdef is None:
            self.macro().error("Error with Nexus Application Definition YAML")
            raise Exception("Error with Nexus Application Definition YAML")

        if self.macro()._name not in self.nxdef.macros:  # TODO find a better way not using _name
            self.macro().warning("Macro not selected in Nexus Application Definition YAML")
            raise Exception(
                "Macro not selected in Nexus Application Definition YAML")

        if self.filename is None:
            self.macro().error("Filename not defined")
            raise Exception("Filename not defined")

    def getFormat(self):
        return 'HDF5::{}'.format(self.nxdef.definition)

    def get_sarenv(self, env_name):
        if self.macro is None:
            raise Exception("Cannot get environment data from macro object")
        try:
            return self.macro().getMacroServer().get_env(env_name)
        except UnknownEnv as e:
            self.macro().error("No {} environment variable found".format(
                self.app_definition_env_name))
            raise e

    def _startRecordList(self, recordlist):
        self.info("enter _startRecordList")

        self.currentlist = recordlist
        env = self.currentlist.getEnviron()
        serialno = env['serialno']
        self._dataCompressionRank = env.get('DataCompressionRank',
                                            self._dataCompressionRank)

        # open/create the file and store its descriptor
        self.fd = self._openFile(self.filename)

        # create an entry for this scan using the scan serial number
        self.entryname = 'entry%d' % serialno

        try:
            nxentry = self.fd.create_group(self.entryname)
        except ValueError:
            # Warn and abort
            if self.entryname in list(self.fd.keys()):
                msg = ('{ename:s} already exists in {fname:s}. '
                       'Aborting macro to prevent data corruption.\n'
                       'This is likely caused by a wrong ScanID\n'
                       'Possible workarounds:\n'
                       '  * first, try re-running this macro (the ScanID '
                       'may be automatically corrected)\n'
                       '  * if not, try changing ScanID with senv, or...\n'
                       '  * change the file name ({ename:s} will be in both '
                       'files containing different data)\n'
                       '\nPlease report this problem.'
                       ).format(ename=self.entryname, fname=self.filename)
                raise RuntimeError(msg)
            else:
                raise
        nxentry.attrs['NX_class'] = 'NXentry'

        # Adapt the datadesc to the NeXus requirements
        self.datadesc = []
        for dd in env['datadesc']:
            dd = dd.clone()
            dd.label = self.sanitizeName(dd.label)
            if not hasattr(dd, "value_ref_enabled"):
                dd.value_ref_enabled = False
            if dd.dtype == 'bool':
                dd.dtype = 'int8'
                self.debug('%r will be stored with type=%r', dd.name, dd.dtype)
            elif dd.dtype == 'str':
                dd.dtype = NXscanH5_FileRecorder.str_dt
            if dd.value_ref_enabled:
                # substitute original data (image or spectrum) type and shape
                # since we will receive references instead
                dd.dtype = NXscanH5_FileRecorder.str_dt
                dd.shape = tuple()
                self.debug('%r will be stored with type=%r', dd.name, dd.dtype)
            if dd.dtype in self.supported_dtypes:
                self.datadesc.append(dd)
            else:
                msg = '%r will not be stored. Reason: %r not supported'
                self.warning(msg, dd.name, dd.dtype)

        # Instruments from YAML
        _instrgrp = nxentry.create_group('instrument')
        _instrgrp.attrs['NX_class'] = 'NXinstrument'

        self._instr_expChan_map = {}
        self._instr_snapshot_map = {}
        for instr in self.nxdef.instruments:
            instrument = _instrgrp.create_group(instr.name)
            instrument.attrs['NX_class'] = instr.klass
            self.iterate_instrument_other_fields(
                instrument, instr.other_fields)

        # Sardana instruments, in case they are used (adapted from NXscan):
        self._nxclass_map = {}
        for inst in env.get('instrumentlist', []):
            self._nxclass_map[_instrgrp.name + inst.getFullName()] = inst.klass
        if self._nxclass_map is {}:
            self.warning('Missing information on NEXUS structure. '
                         + 'Nexus Tree will not be created')

        self.debug('Starting new recording %d on file %s', serialno,
                   self.filename)

        # populate the entry with some data
        nxentry.create_dataset('definition', data=self.nxdef.definition)
        import sardana.release
        program_name = '%s (%s)' % (sardana.release.name,
                                    self.__class__.__name__)
        _pname = nxentry.create_dataset('program_name', data=program_name)
        _pname.attrs['version'] = sardana.release.version
        nxentry.create_dataset(
            'start_time', data=env['scanstarttime'].isoformat())
        nxentry.create_dataset(
            'macro_start_time', data=env['starttime'].isoformat())
        timedelta = (env['starttime'] - datetime(1970, 1, 1))
        _epoch = timedelta.total_seconds()
        nxentry.attrs['epoch'] = _epoch
        nxentry.create_dataset('title', data=env['title'])
        nxentry.create_dataset('entry_identifier', data=str(env['serialno']))

        # _usergrp = nxentry.create_group('user')
        # _usergrp.attrs['NX_class'] = 'NXuser'
        # _usergrp.create_dataset('name', data=env['user'])

        # Experiment info
        expinfo = self.nxdef.getExperimentInfo(self.macro())
        nxentry.create_dataset('proposal_id', data=expinfo.proposal_id)
        nxentry.create_dataset('experiment_identifier', data=expinfo.exp_id)
        nxentry.create_dataset('experiment_description', data=expinfo.exp_desc)
        nxentry.create_dataset('safety_info', data=expinfo.safety_info)
        expteam = nxentry.create_group('experimental_team')

        npers = 0
        for usr in expinfo.exp_team:
            npers = npers + 1
            person = expteam.create_group('person%d' % npers)
            person.attrs['NX_class'] = 'NXuser'
            person.create_dataset(
                'name', data=usr.get('name', '(not provided)'))
            person.create_dataset(
                'email', data=usr.get('email', '(not provided)'))
            person.create_dataset('affiliation', data=usr.get(
                'affiliation', '(not provided)'))
            person.create_dataset(
                'role', data=usr.get('role', '(not provided)'))
            person.create_dataset(
                'orcid', data=usr.get('orcid', '(not provided)'))

        # prepare the 'measurement' group
        _meas = nxentry.create_group('data')
        _meas.attrs['NX_class'] = 'NXdata'
        if self.savemode == SaveModes.Record:
            # create extensible datasets
            for dd in self.datadesc:
                dataset_dest = _meas
                link = False
                if dd.label in self._instr_expChan_map:
                    dataset_dest = self.fd[self._instr_expChan_map[dd.label]
                                           ["nxentry_name"]]
                    # TODO find a better way? Here we check if the expchan is from
                    # an instrument so to put the data there and create a soft link
                    dd.label = self._instr_expChan_map[dd.label]["name"]
                    link = True
                shape = ([0] + list(dd.shape))
                _ds = dataset_dest.create_dataset(
                    dd.label,
                    dtype=dd.dtype,
                    shape=shape,
                    maxshape=([None] + list(dd.shape)),
                    chunks=(1,) + tuple(dd.shape),
                    compression=self._compression(shape)
                )
                if hasattr(dd, 'data_units'):
                    _ds.attrs['units'] = dd.data_units
                if link:
                    _meas[dd.label] = h5py.SoftLink(_ds.name)
        else:
            # leave the creation of the datasets to _writeRecordList
            # (when we actually know the length of the data to write)
            pass

        self._createPreScanSnapshot(env)
        self.fd.flush()

    def iterate_instrument_other_fields(self, parent_nxentry, other_fields_dict):
        for key, value in other_fields_dict.items():
            if isinstance(value, dict):
                self.iterate_instrument_other_fields(
                    parent_nxentry.create_group(key), value)
                continue
            elif isinstance(value, str):
                if is_sarurl(value):
                    prefix, path = split_sarurl(value)
                    if is_sarenv(prefix):
                        envdata = self.get_sarenv(path)
                        if (isinstance(envdata, dict)):
                            self.iterate_instrument_other_fields(
                                parent_nxentry.create_group(path), envdata)
                        else:
                            parent_nxentry.create_dataset(key, data=envdata)
                        continue
                    elif is_mgdata(prefix):
                        # map to create the links afterwards in data
                        self._instr_expChan_map[path] = {
                            "name": key, "nxentry_name": parent_nxentry.name}
                        continue
                    elif is_snapshot(prefix):
                        # map to create the links in pre-scan snapshot
                        self._instr_snapshot_map[path] = {
                            "name": key, "nxentry_name": parent_nxentry.name}
                        continue

            parent_nxentry.create_dataset(key, data=value)

    def _createPreScanSnapshot(self, env):
        """
        Write the pre-scan snapshot in "<entry>/data/pre_scan_snapshot".
        Also link to the corresponding <entry>/instrument if applicable
        """
        nxentry = self.fd[self.entryname]
        _meas = nxentry['data']
        self.preScanSnapShot = env.get('preScanSnapShot', [])
        _snap = _meas.create_group('pre_scan_snapshot')
        _snap.attrs['NX_class'] = 'NXcollection'

        for dd in self.preScanSnapShot:
            label = self.sanitizeName(dd.label)
            if label in _snap:
                self.warning(
                    "PreScanSnapShot: skipping duplicated label'{}'".format(
                        label
                    )
                )
                continue
            dtype = dd.dtype
            pre_scan_value = dd.pre_scan_value
            if dd.dtype == 'bool':
                dtype = 'int8'
                pre_scan_value = numpy.int8(dd.pre_scan_value)
                self.debug('Pre-scan snapshot of %s will be stored as type %s',
                           dd.name, dtype)
            elif dd.dtype == 'str':
                dtype = NXscanH5_FileRecorder.str_dt
                dd.dtype = NXscanH5_FileRecorder.str_dt

            if dtype in self.supported_dtypes:
                # Check if it is part of an instrument (YAML or Sardana)
                dest_group = _snap
                link = False

                # YAML
                if dd.label in self._instr_snapshot_map:
                    dest_group = self.fd[self._instr_snapshot_map[dd.label]
                                         ["nxentry_name"]]
                    dd.label = self._instr_snapshot_map[dd.label]["name"]
                    link = True

                # Sardana
                elif getattr(dd, 'instrument', None):
                    _instrgrp = nxentry.require_group("instrument")
                    _instr = self._createNXpath(dd.instrument,
                                                prefix=_instrgrp.name)
                    dest_group = _instr
                    link = True

                # data in pre-scan snapshot group
                _ds = dest_group.create_dataset(
                    label,
                    data=pre_scan_value,
                    compression=self._compression(dd.shape)
                )
                # create the link
                if link:
                    _snap[dd.label] = h5py.SoftLink(_ds.name)

    def _writeRecord(self, record):

        _meas = self.fd[posixpath.join(self.entryname, 'data')]

        for dd in self.datadesc:
            if dd.name in record.data:
                data = record.data[dd.name]
                _ds = _meas[dd.label]
                if data is None:
                    data = numpy.zeros(dd.shape, dtype=dd.dtype)
                # skip NaN if value reference is enabled
                if dd.value_ref_enabled and not is_pure_str(data):
                    continue
                elif not hasattr(data, 'shape'):
                    data = numpy.array([data], dtype=dd.dtype)
                elif dd.dtype != data.dtype.name:
                    self.debug('%s casted to %s (was %s)',
                               dd.label, dd.dtype, data.dtype.name)
                    data = data.astype(dd.dtype)

                # resize the dataset and add the latest chunk
                if _ds.shape[0] <= record.recordno:
                    _ds.resize(record.recordno + 1, axis=0)

                # write the slab of data
                _ds[record.recordno, ...] = data

            else:
                self.debug('missing data for label %r', dd.label)
        self.fd.flush()

    def _endRecordList(self, recordlist):

        env = self.currentlist.getEnviron()
        nxentry = self.fd[self.entryname]

        for dd, dd_env in zip(self.datadesc, env["datadesc"]):
            label = dd.label

            # If h5file scheme is used: Creation of a Virtual Dataset
            if dd.value_ref_enabled:
                measurement = nxentry['data']
                try:
                    dataset = measurement[label].asstr()
                except AttributeError:
                    # h5py < 3
                    dataset = measurement[label]
                first_reference = dataset[0]
                group = re.match(self.pattern, first_reference)
                if group is None:
                    msg = 'Unsupported reference %s' % first_reference
                    self.warning(msg)
                    continue

                uri_groups = group.groupdict()
                if uri_groups['scheme'] != "h5file":
                    continue
                if not VDS_available:
                    msg = ("VDS not available in this version of h5py, "
                           "{0} will be stored as string reference")
                    msg.format(label)
                    self.warning(msg)
                    continue

                bk_label = "_" + label
                measurement[bk_label] = measurement[label]
                nb_points = measurement[label].size

                (dim_1, dim_2) = dd_env.shape
                layout = h5py.VirtualLayout(shape=(nb_points, dim_1, dim_2),
                                            dtype=dd_env.dtype)

                for i in range(nb_points):
                    reference = dataset[i]
                    group = re.match(self.pattern, reference)
                    if group is None:
                        msg = 'Unsupported reference %s' % first_reference
                        self.warning(msg)
                        continue
                    uri_groups = group.groupdict()
                    filename = uri_groups["filepath"]
                    remote_dataset_name = uri_groups["dataset"]
                    if remote_dataset_name is None:
                        remote_dataset_name = "dataset"
                    vsource = h5py.VirtualSource(filename, remote_dataset_name,
                                                 shape=(dim_1, dim_2))
                    layout[i] = vsource

                # Substitute dataset by Virtual Dataset in output file
                try:
                    del measurement[label]
                    measurement.create_virtual_dataset(
                        label, layout, fillvalue=-numpy.inf)
                except Exception as e:
                    msg = 'Could not create a Virtual Dataset. Reason: %r'
                    self.warning(msg, e)
                else:
                    del measurement[bk_label]

        nxentry.create_dataset('end_time', data=env['endtime'].isoformat())
        self.fd.flush()
        self.debug('Finishing recording %d on file %s:',
                   env['serialno'], self.filename)
        if self._close:
            self.fd.close()
        self.currentlist = None

    def writeRecordList(self, recordlist):
        """Called when in BLOCK writing mode"""
        self._startRecordList(recordlist)
        _meas = self.fd[posixpath.join(self.entryname, 'data')]
        for dd in self.datadesc:
            shape = ([len(recordlist.records)] + list(dd.shape))
            _ds = _meas.create_dataset(
                dd.label,
                dtype=dd.dtype,
                shape=shape,
                chunks=(1,) + tuple(dd.shape),
                compression=self._compression(shape)
            )
            if hasattr(dd, 'data_units'):
                _ds.attrs['units'] = dd.data_units

            for record in recordlist.records:
                if dd.label in record.data:
                    _ds[record.recordno, ...] = record.data[dd.label]
                else:
                    self.debug('missing data for label %r in record %i',
                               dd.label, record.recordno)
        self._endRecordList(recordlist)

    def _addCustomData(self, value, name, nxpath: Optional[str] = None, dtype=None, **kwargs):
        """
        Apart from value and name, this recorder can use the following optional
        parameters:

        :param nxpath: a nexus path (optionally using name:nxclass
                       notation for the group names). See the rules for
                       automatic nxclass resolution used by
                       :meth:`._createNXpath`. If None given, it defaults to
                       nxpath='custom_data:NXcollection'

        :param dtype: name of data type (it is inferred from value if not
                      given)
        """
        if nxpath is None:
            nxpath = 'custom_data:NXcollection'
        if dtype is None:
            if numpy.isscalar(value):
                dtype = numpy.dtype(type(value)).name
                if numpy.issubdtype(dtype, str):
                    dtype = NXscanH5_FileRecorder.str_dt
                if dtype == 'bool':
                    value, dtype = int(value), 'int8'
            else:
                value = numpy.array(value)
                dtype = value.dtype.name

        if dtype not in self.supported_dtypes and dtype != 'char':
            self.warning(
                'cannot write %r. Reason: unsupported data type', name)
            return

        # open the file if necessary
        fileClosed = True
        try:
            if self.fd.id.valid:
                fileClosed = False
        except AttributeError:
            pass
        if fileClosed:
            self.fd = self._openFile(self.filename)

        # create the custom data group if it does not exist
        grp = self._createNXpath(nxpath)
        try:
            grp.create_dataset(name, data=value)
        except ValueError as e:
            msg = 'Error writing %s. Reason: %s' % (name, e)
            self.warning(msg)
            if self.macro is not None:
                self.macro().warning(msg)

        # flush
        self.fd.flush()

        # leave the file as it was
        if fileClosed:
            self.fd.close()
