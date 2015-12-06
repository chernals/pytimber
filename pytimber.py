import os, glob, time, datetime
import threading
import multiprocessing as mp
import jpype
import numpy as np

"""
http://abwww.cern.ch/ap/dist/accsoft/cals/accsoft-cals-extr-client/PRO/build/dist/accsoft-cals-extr-client-nodep.jar
"""

_moddir=os.path.dirname(__file__)
_jar=os.path.join(_moddir,'localJars/accsoft-cals-extr-client-nodep.jar')

if not jpype.isJVMStarted():
    libjvm = jpype.getDefaultJVMPath()
    jpype.startJVM(libjvm,'-Djava.class.path=%s'%_jar)
else:
    print('Warning: JVM already started')

# Definitions of Java packages
cern=jpype.JPackage('cern')
org=jpype.JPackage('org')
java=jpype.JPackage('java')
ServiceBuilder=cern.accsoft.cals.extr.client.service.ServiceBuilder
DataLocationPreferences=cern.accsoft.cals.extr.domain.core.datasource.DataLocationPreferences
VariableDataType=cern.accsoft.cals.extr.domain.core.constants.VariableDataType
Timestamp=java.sql.Timestamp
null=org.apache.log4j.varia.NullAppender()
org.apache.log4j.BasicConfigurator.configure(null)

def toTimestamp(t):
    if type(t) is str:
        return Timestamp.valueOf(t)
    elif type(t) is datetime.datetime:
        return Timestamp.valueOf(t.strftime("%Y-%m-%d %H:%M:%S"))
    elif t is None:
        return None
    else:
        sec=int(t)
        nanos=int((t-sec)*1e9)
        tt=Timestamp(long(sec*1000))
        #tt.setNanos(nanos)
        return tt

def toStringList(myArray):
    myList = java.util.ArrayList()
    for s in myArray:
        myList.add(s)
    return myList

def cleanName(s):
    if s[0].isdigit():
        s = '_'+s
    out = []
    for ss in s:
        if ss in ' _;><':
            out.append('_')
        else:
            out.append(ss)
    return ''.join(out)

source_dict={'mdb' : DataLocationPreferences.MDB_PRO,
             'ldb' : DataLocationPreferences.LDB_PRO,
             'all' : DataLocationPreferences.MDB_AND_LDB_PRO
             }

def processDataset(ds, datatype, with_timestamp):
    start_time = time.time()
    datas = []
    tss = []
    for tt in ds:
        if with_timestamp:
            ts = tt.getStamp()
            ts = datetime.datetime.fromtimestamp(ts.fastTime/1000.)
            tss.append(ts)
        if datatype == 'MATRIXNUMERIC':
            val = np.array(tt.getMatrixDoubleValues())
        elif datatype == 'VECTORNUMERIC':
            val = np.array(tt.getDoubleValues())
        elif datatype == 'NUMERIC':
            val = tt.getDoubleValue()
        elif datatype == 'FUNDAMENTAL':
            val = 1
        else:
            print('Unsupported datatype, returning the java object')
            val = tt
        datas.append(val)
    print("Processing of %s:" % ds.getVariableName(), time.time()-start_time, "seconds")
    if with_timestamp:
        return (tss, datas)
    else:
        return datas
        
class LoggingDB(object):
    def __init__(self, appid='LHC_MD_ABP_ANALYSIS', clientid='BEAM PHYSICS', source='mdb', silent=False):
        loc = source_dict[source]
        self._builder = ServiceBuilder.getInstance(appid, clientid, loc)
        self._md = self._builder.createMetaService()
        self._ts = self._builder.createTimeseriesService()
        self.tree = Hierarchy('root', None, None, self._md)
        self._silent = silent

    def mute(self):
        self._silent = True

    def unmute(self):
        self._silent = False

    def search(self, pattern):
        """
        Search for parameter names.
        Wildcard is '%'.
        """
        types = VariableDataType.ALL
        v = self._md.getVariablesOfDataTypeWithNameLikePattern(pattern, types)
        return v.toString()[1:-1].split(', ')

    def getFundamentals(self, t1, t2, fundamental):
        if not self._silent: print('Querying fundamentals (pattern: {0}):'.format(fundamental))
        fundamentals = self._md.getFundamentalsInTimeWindowWithNameLikePattern(t1, t2, fundamental)
        if fundamentals is None:
            if not self._silent: print('No fundamental found in time window')
        else:
            for f in fundamentals:
                if not self._silent: print(f)
        return fundamentals

    def getVariablesList(self, pattern_or_list, t1, t2):
        """
        Get a list of variables based on a list of strings or a pattern.
        Wildcard for the pattern is '%'.
        Assumes t1 and t2 to be Java TimeStamp objects
        """
        if type(pattern_or_list) is str:
            types = VariableDataType.ALL
            variables = self._md.getVariablesOfDataTypeWithNameLikePattern(pattern_or_list, types)
        elif type(pattern_or_list) is list:
            variables = self._md.getVariablesWithNameInListofStrings(java.util.Arrays.asList(pattern_or_list))
        else:
            variables = None
        return variables

    def getAligned(self, pattern_or_list, t1, t2, fundamental=None):
        print("Multiprocessing version of getAligned")
        ts1 = toTimestamp(t1)
        ts2 = toTimestamp(t2)
        master_variable = None
        self._datasets = []
        self._master_ds = None
        manager = mp.Manager()
        out = manager.dict()
        ps = []

        # Build variable list
        variables = self.getVariablesList(pattern_or_list, ts1, ts2)
        if not self._silent: print('List of variables to be queried:')
        if len(variables) ==  0:
            if not self._silent: print('None found.')
            return {}
        else:
            for i, v in enumerate(variables):
                if i == 0:
                    master_variable = variables.getVariable(0)
                    master_name = master_variable.toString()
                    if not self._silent: print('%s (using as master).' % v)
                else:
                    if not self._silent: print(v)
        
        # Fundamentals
        if fundamental is not None:
            fundamentals = self.getFundamentals(ts1, ts2, fundamental)
            if fundamentals is None:
                return {}

        # Acquire master dataset
        start_time = time.time()
        if fundamental is not None:
            ds=self._ts.getDataInTimeWindowFilteredByFundamentals(master_variable, ts1, ts2, fundamentals)
        else:
            ds=self._ts.getDataInTimeWindow(master_variable, ts1, ts2)
        print("Aqn of master:",time.time()-start_time, "seconds")
        if not self._silent: print('Retrieved {0} values for {1} (master)'.format(ds.size(), master_name))
        out["timestamp"], out[master_name] = processDataset(ds, ds.getVariableDataType().toString(), True)
        self._master_ds = ds
 
        # Acquire aligned data based on master dataset timestamps
        start_time = time.time()
        for v in variables:
            if v == master_name:
                continue
            jvar = variables.getVariable(v)
            t = threading.Thread(target=self.threaded_acq, args=(jvar, ))
            t.start()
        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is main_thread:
                continue
            t.join()
        print("Total aqn time:", time.time()-start_time, "seconds")
            
        # Process the datasets
        start_time = time.time()
        for ds in self._datasets:
            p = mp.Process(target=self.mp_processing, args=(ds, out))
            p.start()
            ps.append(p)
        for p in ps:
            p.join()
        print("Total processing time:", time.time()-start_time, "seconds")
        return out
        
    def mp_processing(self, ds, out):
        out[ds.getVariableName()] = processDataset(ds, ds.getVariableDataType().toString(), False)

    def threaded_acq(self, jvar):
        jpype.attachThreadToJVM()
        start_time = time.time()
        v = jvar.getVariableName()
        ds = self._ts.getDataAlignedToTimestamps(jvar, self._master_ds)
        if not self._silent: print('Retrieved {0} values for {1}'.format(ds.size(), v))
        self._datasets.append(ds)
        print("Aqn in thread:", time.time()-start_time, "seconds")
        
    def get(self, pattern_or_list, t1, t2=None, fundamental=None):
        """
        Query the database for a list of variables or for variables whose name matches a pattern (string).
        If no pattern if given for the fundamental all the data are returned.
        If a fundamental pattern is provided, the end of the time window as to be explicitely provided.
        """
        ts1 = toTimestamp(t1)
        ts2 = toTimestamp(t2)       
        out = {}

        # Build variable list
        variables = self.getVariablesList(pattern_or_list, ts1, ts2)
        if not self._silent: print('List of variables to be queried:')
        if len(variables) is 0:
            if not self._silent: print('None found, aborting.')
            return {}
        else:
            for v in variables:
                if not self._silent: print(v)

        # Fundamentals
        if fundamental is not None and ts2 is None:
            print('Unsupported: if filtering  by fundamentals you must provide and correct time window')
            return {}
        if fundamental is not None:
            fundamentals = self.getFundamentals(ts1, ts2, fundamental)
            if fundamentals is None:
                return {}
           
        # Acquire
        for v in variables:
            jvar = variables.getVariable(v)
            if t2 is None:
                res = [self._ts.getLastDataPriorToTimestampWithinDefaultInterval(jvar, ts1)]
                datatype = res[0].getVariableDataType().toString()
                if not self._silent: print('Retrieved {0} values for {1}'.format(1, jvar.getVariableName()))
            else:
                if fundamental is not None:
                    res = self._ts.getDataInTimeWindowFilteredByFundamentals(jvar, ts1, ts2, fundamentals)
                else:
                    res = self._ts.getDataInTimeWindow(jvar, ts1, ts2)
                datatype = res.getVariableDataType().toString()
                if not self._silent: print('Retrieved {0} values for {1}'.format(res.size(), jvar.getVariableName()))
            out[v] = self.processDataset(res, datatype)
        return out
    
class Hierarchy(object):
    def __init__(self,name,obj,src,varsrc):
        self.name=name
        self.obj=obj
        self.varsrc=varsrc
        if src is not None:
          self.src=src
        
    def _get_childs(self):
        if self.obj is None:
            objs=self.src.getHierachies(1)
        else:
            objs=self.src.getChildHierarchies(self.obj)
        return dict([(cleanName(hh.hierarchyName),hh) for hh in objs])
    
    def __getattr__(self,k):
        if k=='src':
            self.src=self.varsrc.getAllHierarchies()
            return self.src
        elif k=='_dict':
            self._dict=self._get_childs()
            return self._dict
        else:
            return Hierarchy(k,self._dict[k],self.src,self.varsrc)
        
    def __dir__(self):
        return sorted(self._dict.keys())
    
    def __repr__(self):
        if self.obj is None:
          return "<Top Hierarchy>"
        else:
          name=self.obj.getHierarchyName()
          desc=self.obj.getDescription()
          return "<%s: %s>"%(name,desc)
    
    def get_vars(self):
        if self.obj is not None:
          vvv=self.varsrc.getVariablesOfDataTypeAttachedToHierarchy(self.obj,VariableDataType.ALL)
          return vvv.toString()[1:-1].split(', ')
        else:
          return []
