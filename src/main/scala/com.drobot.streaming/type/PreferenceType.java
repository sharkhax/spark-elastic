package com.drobot.streaming.type;

/**
 * Enumeration represents types of preferences to be calculated.
 *
 * @author Uladzislau Drobat
 */
public enum PreferenceType {

    /**
     * Preferences will be calculated by stay duration.
     */
    DURATION_ONLY,

    /**
     * Preferences will be calculated by stay duration and children presence.
     */
    EXTENDED
}
