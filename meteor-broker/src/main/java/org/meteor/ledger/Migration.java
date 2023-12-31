package org.meteor.ledger;

import org.meteor.client.internal.ClientChannel;
public record Migration(int ledger, ClientChannel channel) {
}
