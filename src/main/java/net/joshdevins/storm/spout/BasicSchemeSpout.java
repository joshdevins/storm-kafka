package net.joshdevins.storm.spout;

import java.util.List;

import org.apache.commons.lang3.Validate;

import backtype.storm.spout.Scheme;
import backtype.storm.topology.OutputFieldsDeclarer;

/**
 * A simple {@link BasicSpout} that delegates to a provided {@link Scheme} where possible.
 * 
 * @author Josh Devins
 */
public abstract class BasicSchemeSpout extends BasicSpout {

    private static final long serialVersionUID = -832374927567182090L;

    private final Scheme scheme;

    public BasicSchemeSpout(final Scheme scheme) {

        Validate.notNull(scheme, "Scheme is required");
        this.scheme = scheme;
    }

    /**
     * Delegates to the {@link Scheme}.
     */
    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(scheme.getOutputFields());
    }

    /**
     * Delegates deserialization to the {@link Scheme}.
     */
    protected List<Integer> emit(final byte[] bytes) {
        return emit(scheme.deserialize(bytes));
    }
}
