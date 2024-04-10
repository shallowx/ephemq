package org.meteor.ledger;

import org.meteor.client.ClientChannel;

public record Migration(int ledger, ClientChannel channel) {
}
