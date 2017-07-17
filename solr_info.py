import sys
import collectd
import urllib2
import posixpath
import itertools
try:
    import json
except ImportError:
    import simplejson as json



VERBOSE_LOGGING = True
HOST = "localhost"
PORT = 8983
PYTHON_VER = sys.version_info

cores = []
metrics = {"solr.jvm","solr.node","solr.jetty","solr.core.core0"}

def log_verbose(msg):
    if not VERBOSE_LOGGING:
        return
    collectd.info('solr_info plugin [verbose]: %s' % msg)

def get_response(path):
        url = 'http://%s:%i/%s' % (
            HOST, int(PORT), path)
        try:
            response = urllib2.urlopen(url)
        except Exception, err:
            log_verbose("%s: %s" % (url, err))
            return False

        try:
            return json.load(response)
        except (TypeError, ValueError):
            log_verbose("Unable to parse response from solr as a"
                           " json object")
            return False


result = get_response('solr/admin/cores?action=STATUS&wt=json')
if result:
    cores = result['status'].keys()

SOLR_URL = "http://%s:%i/solr" % (HOST, PORT)
SOLR_HANDLERS = {"searchHandler": "org.apache.solr.handler.component.SearchHandler", "mbeans": "/admin/mbeans", 
		"org.apache.solr.handler.DumpRequestHandler": "org.apache.solr.handler.DumpRequestHandler","solr_query": "solr_query",
		"/admin/file": "/admin/file", "/admin/logging": "/admin/logging","/admin/plugins": "/admin/plugins","/admin/luke": "/admin/luke",
		"/update": "/update","/admin/system": "/admin/system","/debug/dump": "/debug/dump","/admin/threads": "/admin/threads",
		"com.datastax.bdp.search.solr.handler.component.CqlSearchHandler": "com.datastax.bdp.search.solr.handler.component.CqlSearchHandler",
		"org.apache.solr.handler.admin.InfoHandler": "org.apache.solr.handler.admin.InfoHandler",
		"org.apache.solr.handler.UpdateRequestHandler": "org.apache.solr.handler.UpdateRequestHandler",
		"org.apache.solr.handler.PingRequestHandler": "org.apache.solr.handler.PingRequestHandler",
		"/admin/properties": "/admin/properties","search": "search","/update/json/docs": "/update/json/docs",
		"com.datastax.bdp.search.solr.handler.admin.CassandraCoreAdminHandler": "com.datastax.bdp.search.solr.handler.admin.CassandraCoreAdminHandler",
		"/admin/ping": "/admin/ping", "org.apache.solr.handler.component.SearchHandler": "org.apache.solr.handler.component.SearchHandler"}

def dispatch_value(value, value_name, value_type, type_instance=None, core=""):
    plugin = "%s_solr_info" % (core)   

    val = collectd.Values(plugin=plugin)
    val.type = value_type
    if type_instance is not None:
       val.plugin_instance = value_name
       val.type_instance = type_instance
    else:
       val.type_instance = value_name
    val.values = [value]
    val.dispatch()

def fetch_metrics():
    stats_url = "solr/admin/metrics?wt=json" 
    
    solr_data = get_response(stats_url)
    # Searcher information
    solr_data = solr_data["metrics"]
    data = { "solr.jvm": {}, "solr.node": {}, "solr.jetty": {}, "solr.core.core0": {}}
    for metric in metrics:
        for key,value in solr_data[metric].items():
		data[metric][key] = {}
		for internal_key, internal_value in value.items():
			if type(internal_value) is not dict:
				if isinstance(value[internal_key], (int, float)):
					data[metric][key][internal_key]=value[internal_key]
			else:
				data[metric][key][internal_key]={}
				for inner_internal_key, inner_internal_value in internal_value.items():
					if isinstance(internal_value[inner_internal_key], (int, float)):
						data[metric][key][internal_key][inner_internal_key]=internal_value[inner_internal_key]

    return data

def fetch_core_data(core):
    global SOLR_HANDLERS
    stats_url = "solr/%s/admin/mbeans?stats=true&wt=json" % (core)
    solr_data = get_response(stats_url)

    # Searcher information
    solr_data = solr_data["solr-mbeans"]
    
    # Data is return in form of [ "TYPE", { DATA }, "TYPE", ... ] so pair them up
    solr_data_iter = iter(solr_data)
    solr_data = itertools.izip(solr_data_iter, solr_data_iter)
    
    data = { "docs": {}, "cache": {}, "handler_stats": {}, "update_stats": {} }
    for module, module_data in solr_data:
        if module == "CORE":
            data["docs"]["num_docs"] = module_data["searcher"]["stats"]["numDocs"]
	    data["docs"]["max_doc"] = module_data["searcher"]["stats"]["maxDoc"]
            data["docs"]["warm_up_time"] = module_data["searcher"]["stats"]["warmupTime"]
        elif module == "CACHE":
            data["cache"]["size"] = module_data["fieldValueCache"]["stats"]["size"]
            data["cache"]["hitratio"] = module_data["fieldValueCache"]["stats"]["hitratio"]
            data["cache"]["evictions"] = module_data["fieldValueCache"]["stats"]["evictions"]
        elif module == "QUERYHANDLER":
	    if PYTHON_VER >= (2,7):
            	interesting_handlers = { endpoint: name for name, endpoint in SOLR_HANDLERS.iteritems() }
	    else:
                interesting_handlers = {}
                for name, endpoint in SOLR_HANDLERS.iteritems():
                    interesting_handlers[name] = endpoint
            for handler, handler_data in module_data.iteritems():
                if handler not in interesting_handlers:
                    continue
                
            	handler_name = interesting_handlers[handler]
            	data["handler_stats"][handler_name] = {}
            	data["handler_stats"][handler_name]["requests"] = handler_data["stats"]["requests"]
            	data["handler_stats"][handler_name]["errors"] = handler_data["stats"]["errors"] 
            	data["handler_stats"][handler_name]["timeouts"] = handler_data["stats"]["timeouts"]
            	data["handler_stats"][handler_name]["time_per_request"] = handler_data["stats"]["avgTimePerRequest"]                
            	data["handler_stats"][handler_name]["requests_per_second"] = handler_data["stats"]["avgRequestsPerSecond"]
        elif module == "UPDATEHANDLER":
            data["update_stats"]["commits"] = module_data["updateHandler"]["stats"]["commits"]
            data["update_stats"]["autocommits"] = module_data["updateHandler"]["stats"]["autocommits"]
            data["update_stats"]["soft_autocommits"] = module_data["updateHandler"]["stats"]["soft autocommits"]
            data["update_stats"]["optimizes"] = module_data["updateHandler"]["stats"]["optimizes"]
            data["update_stats"]["rollbacks"] = module_data["updateHandler"]["stats"]["rollbacks"]
            data["update_stats"]["expunges"] = module_data["updateHandler"]["stats"]["expungeDeletes"]
            data["update_stats"]["pending_docs"] = module_data["updateHandler"]["stats"]["docsPending"]
            data["update_stats"]["adds"] = module_data["updateHandler"]["stats"]["adds"]
            data["update_stats"]["deletes_by_id"] = module_data["updateHandler"]["stats"]["deletesById"]
            data["update_stats"]["deletes_by_query"] = module_data["updateHandler"]["stats"]["deletesByQuery"]
            data["update_stats"]["errors"] = module_data["updateHandler"]["stats"]["errors"]
    return data

def configure_callback(conf):
        global HOST, PORT
        for node in conf.children:
            	if node.key == 'Host':
                	HOST = node.values[0]
                elif node.key == 'Port':
                	PORT = int(node.values[0])
		else:
			log_verbose("Error: Invalid key in collectd solr plugin config file."
				    " Valid Keys are Host, Port")
			sys.exit()

def read_callback():
    metric_data = fetch_metrics()
    for metric in metrics:
	for key,value in metric_data[metric].items():
		for internal_key, internal_value in value.items():
			if type(internal_value) is not dict:
                                dispatch_value(metric_data[metric][key][internal_key],metric,"gauge",metric + "." + key + "." + internal_key)
                        else:
                                for inner_internal_key, inner_internal_value in internal_value.items():
                                        dispatch_value(metric_data[metric][key][internal_key][inner_internal_key],metric,"gauge",metric + "." + key + "." + internal_key + "." +inner_internal_key)

    for core in cores:
        ping_url = posixpath.normpath("solr/{0}/admin/ping?wt=json".format(core))
	result = get_response(ping_url)
	if not result:
		continue        
    	data = fetch_core_data(core)
    	dispatch_value(data["docs"]["num_docs"], "documents", "gauge", "numDocs", core)
    	dispatch_value(data["docs"]["max_doc"], "documents", "gauge", "maxDoc", core)
	dispatch_value(data["docs"]["warm_up_time"], "documents", "gauge", "warmupTime", core)
    	dispatch_value(data["cache"]["size"], "cache", "gauge", "size", core)
    	dispatch_value(data["cache"]["hitratio"], "cache", "gauge", "hitratio", core)
    	dispatch_value(data["cache"]["evictions"], "cache", "gauge", "evictions", core)

    	for handler_name, handler_data in data["handler_stats"].iteritems():
        	dispatch_value(handler_data["requests"], handler_name, "gauge", "requests", core)
        	dispatch_value(handler_data["errors"], handler_name, "gauge", "errors", core)
        	dispatch_value(handler_data["timeouts"], handler_name, "gauge", "timeouts", core)
        	dispatch_value(handler_data["time_per_request"], "request_times", "gauge", handler_name, core)
        	dispatch_value(handler_data["requests_per_second"], "requests_per_second", "gauge", handler_name, core)

    	dispatch_value(data["update_stats"]["commits"], "update", "gauge", "commits", core)
    	dispatch_value(data["update_stats"]["autocommits"], "update", "gauge", "autocommits", core)
    	dispatch_value(data["update_stats"]["soft_autocommits"], "update", "gauge", "soft_autocommits", core)
    	dispatch_value(data["update_stats"]["optimizes"], "update", "gauge", "optimizes", core)
    	dispatch_value(data["update_stats"]["expunges"], "update", "gauge", "expunges", core)
    	dispatch_value(data["update_stats"]["rollbacks"], "update", "gauge", "rollbacks", core)
    	dispatch_value(data["update_stats"]["pending_docs"], "update", "gauge", "pending_docs", core)
    	dispatch_value(data["update_stats"]["adds"], "update", "gauge", "adds", core)
    	dispatch_value(data["update_stats"]["deletes_by_id"], "update", "gauge", "deletes_by_id", core)
    	dispatch_value(data["update_stats"]["deletes_by_query"], "update", "gauge", "deletes_by_query", core)
    	dispatch_value(data["update_stats"]["errors"], "update", "gauge", "errors", core)

collectd.register_config(configure_callback)
collectd.register_read(read_callback)
