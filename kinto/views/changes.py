from collections import defaultdict

from cornice import Service
from pyramid import httpexceptions
from pyramid.security import NO_PERMISSION_REQUIRED


changes = Service(name='changes',
                  description='Clear database content',
                  path='/changes')


@changes.get(permission=NO_PERMISSION_REQUIRED)
def changes_get(request):
    collections = defaultdict(int)

    def get_all(**kw):
        return request.registry.storage.get_all(**kw)[0]

    for bucket in get_all(collection_id='bucket', parent_id=''):
        b_parent_id = '/buckets/%s' % bucket['id']

        for collection in get_all(collection_id='collection',
                                  parent_id=b_parent_id):

            collection_id = collection['id']
            c_parent_id = '%s/collections/%s' % (b_parent_id, collection_id)

            for record in get_all(collection_id='record',
                                  parent_id=c_parent_id):
                if record['last_modified'] > collections[collection_id]:
                    collections[collection_id] = record['last_modified']

    # XXX add a filter so we display only the latest changes
    return {'data': [{'id': colid, 'last_modified': col['last_modified']}
                     for colid, col in collections.items()
                    ]}
