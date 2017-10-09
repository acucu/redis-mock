package ai.grakn.redismock.commands;

import ai.grakn.redismock.RedisBase;
import ai.grakn.redismock.Response;
import ai.grakn.redismock.Slice;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class RO_keys extends AbstractRedisOperation {
    RO_keys(RedisBase base, List<Slice> params) {
        super(base, params, 1, null, null);
    }

    Slice response() {
        List<Slice> matchingKeys = new ArrayList<>();
        String regex = createRegexFromGlob(new String(params().get(0).data(), StandardCharsets.UTF_8));

        base().keys().forEach(keyData -> {
            String key = new String(keyData.data(), StandardCharsets.UTF_8);
            if(key.matches(regex)){
                matchingKeys.add(Response.bulkString(keyData));
            }
        });

        return Response.array(matchingKeys);
    }

    private static String createRegexFromGlob(String glob)
    {
        String out = "^";
        for(int i = 0; i < glob.length(); ++i)
        {
            final char c = glob.charAt(i);
            switch(c)
            {
                case '*': out += ".*"; break;
                case '?': out += '.'; break;
                case '.': out += "\\."; break;
                case '\\': out += "\\\\"; break;
                default: out += c;
            }
        }
        out += '$';
        return out;
    }
}
