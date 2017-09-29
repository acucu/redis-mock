package ai.grakn.redismock;

import ai.grakn.redismock.expecptions.EOFException;
import ai.grakn.redismock.expecptions.ParseErrorException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisCommandParser {


    @VisibleForTesting
    static RedisCommand parse(String stringInput) throws ParseErrorException, EOFException {
        Preconditions.checkNotNull(stringInput);

        return parse(new ByteArrayInputStream(stringInput.getBytes(StandardCharsets.UTF_8)));
    }

    static RedisCommand parse(InputStream messageInput) throws ParseErrorException, EOFException {
        Preconditions.checkNotNull(messageInput);

        long count = SliceParser.consumeCount(messageInput);
        if (count == 0) {
            throw new ParseErrorException();
        }
        RedisCommand command = new RedisCommand();
        for (long i = 0; i < count; i++) {
            command.addParameter(SliceParser.consumeParameter(messageInput));
        }
        return command;
    }
}
