package net.joshdevins.storm.spout;

import java.nio.charset.Charset;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Encodes a byte array into a single UTF-8 string. Very useful for testing and passing raw JSON messages around without
 * proper deserialization.
 * 
 * @see <a
 *      href="https://github.com/nathanmarz/storm-kestrel/blob/master/src/jvm/backtype/storm/scheme/StringScheme.java">StringScheme
 *      from storm-kestrel</a>
 * 
 * @author Josh Devins
 */
public class StringScheme implements Scheme {

    private static final long serialVersionUID = -288263771422595910L;

    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public List<Object> deserialize(final byte[] bytes) {
        return new Values(new String(bytes, UTF8));
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("payload");
    }

}
