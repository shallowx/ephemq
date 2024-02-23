package org.meteor.ledger;

import org.meteor.client.internal.ClientChannel;

record Migration(int ledger, ClientChannel channel) {
}
