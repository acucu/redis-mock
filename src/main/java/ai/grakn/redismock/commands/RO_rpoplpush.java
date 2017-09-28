package ai.grakn.redismock.commands;

import ai.grakn.redismock.RedisBase;
import ai.grakn.redismock.Response;
import ai.grakn.redismock.Slice;

import java.util.List;

class RO_rpoplpush extends AbstractRedisOperation {
    RO_rpoplpush(RedisBase base, List<Slice> params) {
        super(base, params, 2, null, null);
    }

    @Override
    public Slice execute() {
        Slice list1Key = params().get(0);
        Slice list2Key = params().get(1);

        //Pop last one
        //TODO: pop stuff

        return Response.bulkString(base().rawGet(params().get(0)));
    }
}
