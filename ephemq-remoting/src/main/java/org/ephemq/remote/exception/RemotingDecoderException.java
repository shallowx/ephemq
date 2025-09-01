package org.ephemq.remote.exception;

import io.netty.buffer.ByteBuf;
import org.ephemq.remote.codec.MessagePacket;

import java.io.Serial;

/**
 * A {@code RemotingDecoderException} is thrown to indicate that an error has occurred
 * during the decoding of a remote message.
 *
 * <p>This exception is typically used in scenarios where the decoding of
 * a message buffer fails due to invalid content, such as an incorrect magic number
 * or message length.</p>
 *
 * <p>Typical usage is to throw this exception within a method that decodes
 * a {@link ByteBuf} into a custom message object like {@link MessagePacket}.</p>
 */
public class RemotingDecoderException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 6926716840699621852L;

    /**
     * Constructs a new {@code RemotingDecoderException} with {@code null}
     * as its detail message. The cause is not initialized, and may subsequently
     * be initialized by a call to {@link Throwable#initCause(Throwable)}.
     */
    public RemotingDecoderException() {
    }

    /**
     * Constructs a new {@code RemotingDecoderException} with the specified detail message
     * and cause.
     *
     * @param message the detail message, which is saved for later retrieval by
     *                the {@link Throwable#getMessage()} method.
     * @param cause   the cause, which is saved for later retrieval by the
     *                {@link Throwable#getCause()} method. A null value is permitted,
     *                and indicates that the cause is nonexistent or unknown.
     */
    public RemotingDecoderException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new {@code RemotingDecoderException} with the specified detail message.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the {@link Throwable#getMessage()} method.
     */
    public RemotingDecoderException(String message) {
        super(message);
    }

    /**
     * Constructs a new RemotingDecoderException with the specified cause.
     *
     * @param cause the cause of the exception, which is saved for later retrieval by the {@link Throwable#getCause()} method.
     */
    public RemotingDecoderException(Throwable cause) {
        super(cause);
    }
}
