#
# atlas.py
#
# Classes useful in working with MongoDB Atlas
#
import json
import os
import time
from collections import UserDict

import requests
import urllib3

MONGODB_ATLAS_PUBLIC_KEY = os.getenv("MONGODB_ATLAS_PUBLIC_KEY")
MONGODB_ATLAS_PRIVATE_KEY = os.getenv("MONGODB_ATLAS_PRIVATE_KEY")


class AtlasAPI(object):
    def __init__(
        self,
        orgid=None,
        project=None,
        orgpub=MONGODB_ATLAS_PUBLIC_KEY,
        orgpriv=MONGODB_ATLAS_PRIVATE_KEY,
    ):
        if None in [orgpub, orgpriv]:
            raise Exception(
                "You must have MONGODB_ATLAS_PUBLIC_KEY and MONGODB_ATLAS_PRIVATE_KEY exported in your environment or supply values for orgpub and orgpriv."
            )
        self.orgpub = orgpub
        self.orgpriv = orgpriv
        self.orgid = orgid
        self.project = project
        self.apiurl = "https://cloud.mongodb.com"

    def _project(self, project):
        """Check to see if the given parameter is not None, and if so try to
        fall back to the value passed in during class initialization.
        """
        if project is None and self.project is None:
            return None
        elif project is None:
            return self.project
        else:
            return project

    def post(self, thisurl, data):
        """Generic POST action. Data should be a dict."""
        while thisurl[0] == "/":
            thisurl = thisurl[1:]

        thisurl = f"{self.apiurl}/{thisurl}"
        digest = requests.auth.HTTPDigestAuth(self.orgpub, self.orgpriv)

        return requests.post(thisurl, auth=digest, json=data,)

    def patch(self, thisurl, data):
        """Generic PATCH action. Data should be a dict."""
        while thisurl[0] == "/":
            thisurl = thisurl[1:]

        thisurl = f"{self.apiurl}/{thisurl}"
        digest = requests.auth.HTTPDigestAuth(self.orgpub, self.orgpriv)

        return requests.patch(thisurl, auth=digest, json=data,)

    def get(self, thisurl):
        """Generic GET action."""
        while thisurl[0] == "/":
            thisurl = thisurl[1:]

        thisurl = f"{self.apiurl}/{thisurl}"
        digest = requests.auth.HTTPDigestAuth(self.orgpub, self.orgpriv)

        return requests.get(
            thisurl, auth=digest, headers={"Accept": "application/json"}
        )

    def get_measurement(
        self,
        name,
        granularity="PT1M",
        period="PT1H",
        host=None,
        cluster=None,
        project=None,
    ):
        """
        MongoDB Atlas claims that they support ISO-8601 time and duration
        formats, but they do not include the standard or summary documentation
        on how those work. However, I found this for the period format, which
        is used for both the granularity and period parameters:
            https://www.digi.com/resources/documentation/digidocs/90001437-13/reference/r_iso_8601_duration_format.htm
        """
        port = 27017
        project = self._project(project)
        if project is None:
            raise Exception("project is None")

        # If the host was provided, we will just get the measurement for that
        # one host. If the cluster was provided, we'll get the measurement for
        # all hosts in the cluster. Otherwise, we'll get the measurement for
        # all hosts in all clusters in the project.
        baseurl = f"/api/atlas/v1.0/groups/{project}/processes"
        query = f"m={name}&granularity={granularity}&period={period}"

        if host is not None:
            url = f"{baseurl}/{host}:{port}/measurements?{query}"
            r = self.get(url)
            if r.status_code != 200:
                raise Exception(f"{r.status_code} {r.content}")
            return r.json()

        if cluster is not None:
            results = []
            for host in self.gethosts(cluster):
                url = f"{baseurl}/{host}:{port}/measurements?{query}"
                r = self.get(url)
                if r.status_code != 200:
                    raise Exception(f"{r.status_code} {r.content}")
                results += [r.json()]

            return results

        # Default to getting the measurement for all hosts in all clusters in
        # the project.
        results = []
        for cluster in self.getclusterids(project):
            for host in self.gethosts(cluster):
                url = f"{baseurl}/{host}:{port}/measurements?{query}"
                r = self.get(url)
                if r.status_code != 200:
                    raise Exception(f"{r.status_code} {r.content}")
                results += [r.json()]

        return results

    def getclusters(self, project=None):
        """Return a list of clusters in the project"""
        project = self._project(project)
        if project is None:
            raise Exception("project is None")

        url = f"{self.apiurl}/api/atlas/v1.0/groups/{project}/clusters"
        digest = requests.auth.HTTPDigestAuth(self.orgpub, self.orgpriv)

        r = requests.get(
            url, auth=digest, headers={"Accept": "application/json"}
        )
        if r.status_code != 200:
            raise Exception(f"{r.status_code} {r.content}")

        return json.loads(r.content.decode())["results"]

    def getclusterids(self, project=None):
        """Return a list of IDs for clusters in the project"""
        project = self._project(project)
        if project is None:
            raise Exception("project is None")

        url = f"{self.apiurl}/api/atlas/v1.0/groups/{project}/clusters"
        digest = requests.auth.HTTPDigestAuth(self.orgpub, self.orgpriv)

        r = requests.get(
            url, auth=digest, headers={"Accept": "application/json"}
        )
        if r.status_code != 200:
            raise Exception(f"{r.status_code} {r.content}")

        return [x["id"] for x in json.loads(r.content.decode())["results"]]

    def gethosts(self, cluster=None):
        """Retrieve a list of all hosts for the given cluster or all clusters
        in the project.

        cluster: the cluster ID
        """
        res = self.getclusters()

        hosts = []
        for c in res:
            if cluster is not None and c["id"] != cluster:
                continue
            uri = c["mongoURI"]
            for url in uri.split(","):
                tup = urllib3.util.url.parse_url(url)
                hosts += [tup.host]

        return hosts

    def setldap(
        self,
        project,
        binduser,
        bindpass,
        hostname,
        usertodnmapping=None,
        port=636,
    ):
        """Apply the given AtlasLDAPConfig to the given project.

        project: Atlas project ID
        binduser: DN for the account to use for binding to the LDAP server
        bindpass: password for the bind user
        hostname: FQDN for the LDAP endpoint
        usertodnmapping: JSON with 'ldapQuery' and 'match' attributes (see Atlas docs)
        port: 636 for TLS or 389 for unencrypted (default is 636)
        """
        # STEP 1: Verify the configuration
        thisurl = f"/api/atlas/v1.0/groups/{project}/userSecurity/ldap/verify"
        config = {
            "bindUsername": binduser,
            "bindPassword": bindpass,
            "hostname": hostname,
            "port": port,
        }
        r = self.post(thisurl, config)

        # Poll the response URL until the status changes to something other
        # than PENDING.
        requestid = r.json()["requestId"]
        status = r.json()["status"]
        respurl = f"{thisurl}/{requestid}"
        while status == "PENDING":
            time.sleep(1)
            r = self.get(respurl)
            status = r.json()["status"]

        if status != "SUCCESS":
            return (False, r.json()["validations"])

        # STEP 2: Load the config into Atlas
        config = {
            "customerX509": {},
            "ldap": {
                "authenticationEnabled": true,
                "authorizationEnabled": true,
                "bindUsername": binduser,
                "bindPassword": bindpass,
                "hostname": hostname,
                "port": port,
                "userToDNMapping": usertodnmapping,
            },
        }
        r = self.patch(thisurl, config)
        if r.status_code != 202:
            raise Exception(f"{r.status_code} {r.content}")

        return r.json()
