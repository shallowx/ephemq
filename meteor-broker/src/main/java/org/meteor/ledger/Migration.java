package org.meteor.ledger;

import org.meteor.client.core.ClientChannel;

/**
 * Represents the migration process containing a ledger and a client channel.
 * This class is a record that holds the information related to a migration operation.
 *
 * @param ledger  The ledger identifier associated with the migration.
 * @param channel The client channel involved in the migration process.
 */
public record Migration(int ledger, ClientChannel channel) {

    @Override
    public String toString() {
        return STR."(ledger=\{ledger}, channel=\{channel})";
    }
}
