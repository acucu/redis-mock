package ai.grakn.redismock.commands;

import ai.grakn.redismock.RedisBase;
import ai.grakn.redismock.RedisCommand;
import ai.grakn.redismock.Response;
import ai.grakn.redismock.Slice;
import ai.grakn.redismock.expecptions.WrongNumberOfArgumentsException;
import ai.grakn.redismock.expecptions.WrongValueTypeException;
import com.google.common.base.Preconditions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisOperationExecutor {

    private final RedisBase base;
    private List<RedisOperation> transaction;

    public RedisOperationExecutor(RedisBase base) {
        this.base = base;
    }

    public RedisOperation buildSimpleOperation(String name, List<Slice> params){
        switch(name){
            case "set":
                return new RO_set(base, params);
            case "setex":
                return new RO_setex(base, params);
            case "psetex":
                return new RO_psetex(base, params);
            case "setnx":
                return new RO_setnx(base, params);
            case "setbit":
                return new RO_setbit(base, params);
            case "append":
                return new RO_append(base, params);
            case "get":
                return new RO_get(base, params);
            case "getbit":
                return new RO_getbit(base, params);
            case "ttl":
                return new RO_ttl(base, params);
            case "pttl":
                return new RO_pttl(base, params);
            case "expire":
                return new RO_expire(base, params);
            case "pexpire":
                return new RO_pexpire(base, params);
            case "incr":
                return new RO_incr(base, params);
            case "incrby":
                return new RO_incrby(base, params);
            case "decr":
                return new RO_decr(base, params);
            case "decrby":
                return new RO_decrby(base, params);
            case "pfcount":
                return new RO_pfcount(base, params);
            case "pfadd":
                return new RO_pfadd(base, params);
            case "pfmerge":
                return new RO_pfmerge(base, params);
            case "mget":
                return new RO_mget(base, params);
            case "mset":
                return new RO_mset(base, params);
            case "getset":
                return new RO_getset(base, params);
            case "strlen":
                return new RO_strlen(base, params);
            case "del":
                return new RO_del(base, params);
            case "exists":
                return new RO_exists(base, params);
            case "expireat":
                return new RO_expireat(base, params);
            case "pexpireat":
                return new RO_pexpireat(base, params);
            case "lpush":
                return new RO_lpush(base, params);
            case "rpush":
                return new RO_rpush(base, params);
            case "lpushx":
                return new RO_lpushx(base, params);
            case "lrange":
                return new RO_lrange(base, params);
            case "llen":
                return new RO_llen(base, params);
            case "lpop":
                return new RO_lpop(base, params);
            case "rpop":
                return new RO_rpop(base, params);
            case "lindex":
                return new RO_lindex(base, params);
            case "rpoplpush":
            case "brpoplpush":
                return new RO_rpoplpush(base, params);
            default:
                throw new UnsupportedOperationException(String.format("Unsupported operation '%s'", name));
        }
    }

    public synchronized Slice execCommand(RedisCommand command) {
        Preconditions.checkArgument(command.getParameters().size() > 0);

        List<Slice> params = command.getParameters();
        List<Slice> commandParams = params.subList(1, params.size());
        String name = new String(params.get(0).data(), StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);

        try {
            //Transaction handling
            if(name.equals("multi")){
                newTransaction();
            } else if(name.equals("exec")){
                return commitTransaction();
            }


            //Checking if we mutating the transaction or the base
            RedisOperation redisOperation = buildSimpleOperation(name, commandParams);
            if(transaction != null){
                transaction.add(redisOperation);
            } else {
                return redisOperation.execute();
            }

            return Response.OK;
        } catch(UnsupportedOperationException | WrongValueTypeException e){
            return Response.error(e.getMessage());
        } catch (WrongNumberOfArgumentsException e){
            return Response.error(String.format("ERR wrong number of arguments for '%s' command", name));
        }
    }

    private synchronized void newTransaction(){
        if(transaction != null) throw new RuntimeException("Redis mock does not support more than one transaction");
        transaction = new ArrayList<>();
    }

    private synchronized Slice commitTransaction(){
        if(transaction == null) throw new RuntimeException("No transaction started");
        List<Slice> results = transaction.stream().map(RedisOperation::execute).collect(Collectors.toList());
        closeTransaction();
        return Response.array(results);
    }

    private synchronized void closeTransaction(){
        if (transaction != null){
            transaction.clear();
            transaction = null;
        }
    }
}
