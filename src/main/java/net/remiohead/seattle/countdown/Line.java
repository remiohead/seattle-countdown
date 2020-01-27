package net.remiohead.seattle.countdown;

import org.immutables.value.Value;

@Value.Immutable
public interface Line {

    int getKey();
    String getLine();
}
