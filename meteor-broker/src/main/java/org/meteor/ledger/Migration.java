package org.meteor.ledger;

import org.meteor.client.core.ClientChannel;

public record Migration(int ledger, ClientChannel channel) {
}
