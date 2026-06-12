package org.tester.runner;

/**
 * Consistent virtual-user identifier formatting across runners and scalers.
 */
final class VirtualUserIds {

    private VirtualUserIds() {
    }

    static String format(String personaName, int localUserId) {
        return personaName + "-User-" + localUserId;
    }
}
