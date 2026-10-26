/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Provenance: vendored from Axon Framework (https://github.com/AxonFramework/AxonFramework),
// main branch at 5.2.0-SNAPSHOT, commit 22a9fd3f6a9c (2026-04-07),
// messaging/src/main/java/org/axonframework/messaging/commandhandling/distributed/DelegatingCommandBusConnector.java.
// Original package: org.axonframework.messaging.commandhandling.distributed.
// Modified: only the package declaration, changed to the one below. The Apache License 2.0
// header above is the original; see the NOTICE file in the repository root.

package io.kronosdb.connector.axon.distributed.command;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link CommandBusConnector} implementation that wraps another {@link CommandBusConnector} and delegates all calls
 * to it. This can be used to add additional functionality through decoration to a {@link CommandBusConnector} without
 * having to implement all methods again.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public abstract class DelegatingCommandBusConnector implements CommandBusConnector {

    protected final CommandBusConnector delegate;

    /**
     * Initialize the WrappedConnector to delegate all calls to the given {@code delegate}.
     *
     * @param delegate The {@link CommandBusConnector} to delegate all calls to.
     */
    protected DelegatingCommandBusConnector(CommandBusConnector delegate) {
        this.delegate = Objects.requireNonNull(delegate, "The delegate must not be null.");
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        return delegate.dispatch(command, processingContext);
    }

    // region [Connector]
    @Override
    public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
        return delegate.subscribe(commandName, loadFactor);
    }

    @Override
    public boolean unsubscribe(QualifiedName commandName) {
        return delegate.unsubscribe(commandName);
    }

    @Override
    public void onIncomingCommand(Handler handler) {
        delegate.onIncomingCommand(handler);
    }

    // endregion

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
    }
}