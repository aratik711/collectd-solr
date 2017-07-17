# collectd-solr
Add <b>solr_info.py</b> in <b>/usr/share/collectd/collectd-solr/</b>

Then add <b>solr.conf</b> in <b>/etc/collectd.d</b> as follows:
```
LoadPlugin python
<Plugin python>
  ModulePath "/usr/share/collectd/collectd-solr"
  Import "solr_info"
  <Module "solr_info">
    Host "localhost"
    Port 8983
  </Module>
</Plugin>
```
Restart collectd service.
