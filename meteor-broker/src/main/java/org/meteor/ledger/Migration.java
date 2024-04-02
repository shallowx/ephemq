package org.meteor.ledger;

import org.meteor.client.ClientChannel;

record Migration(int ledger, ClientChannel channel) {
}
