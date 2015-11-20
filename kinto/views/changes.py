from cornice import Service
from pyramid import httpexceptions
from pyramid.security import NO_PERMISSION_REQUIRED


changes = Service(name='changes',
                  description='Returns changes',
                  path='/changes')


@changes.get(permission=NO_PERMISSION_REQUIRED)
def changes_get(request):
    res = {}

    def get_all(**kw):
        return request.registry.storage.get_all(**kw)[0]

    for bucket in get_all(collection_id='bucket', parent_id=''):
        bucket_id = bucket['id']
        b_parent_id = '/buckets/%s' % bucket_id

        for collection in get_all(collection_id='collection',
                                  parent_id=b_parent_id):
            collection_id = collection['id']
            cid = '%s/collections/%s' % (b_parent_id, collection_id)

            for record in get_all(collection_id='record',
                                  parent_id=cid):
                if cid in res:
                    res[cid]['last_modified'] = max(record['last_modified'],
                                                    res[cid]['last_modified'])
                else:
                    res[cid] = {'last_modified': record['last_modified'],
                                'bucket_id': bucket_id,
                                'collection_id': collection_id}

    # XXX add a filter so we display only the latest changes
    return {'data': list(res.values())}
