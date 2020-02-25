#
# atlas.py
#
# Classes useful in working with MongoDB Atlas
#
import json
import requests


class AtlasAPI(object):
    def __init__(self, orgpub, orgpriv, orgid, project):
        self.orgpub = orgpub
        self.orgpriv = orgpriv
        self.orgid = orgid
        self.project = project
        self.apiurl = "https://cloud.mongodb.com"

    def post(self, thisurl, data):
        """Generic POST action. Data should be a dict."""
        while thisurl[0] == '/':
            thisurl = thisurl[1:]

        thisurl = f"{self.apiurl}/{thisurl}"
        digest = requests.auth.HTTPDigestAuth(self.orgpub, self.orgpriv)

        return requests.post(
            thisurl,
            auth=digest,
            data=data,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )

    def get(self, thisurl):
        """Generic GET action."""
        while thisurl[0] == '/':
            thisurl = thisurl[1:]

        thisurl = f"{self.apiurl}/{thisurl}"
        digest = requests.auth.HTTPDigestAuth(self.orgpub, self.orgpriv)

        return requests.get(
            thisurl, auth=digest, headers={"Accept": "application/json"}
        )
