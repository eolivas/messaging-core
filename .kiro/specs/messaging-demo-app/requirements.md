# Requirements Document

## Introduction

The Messaging Demo App is a demonstration application that shows the Messaging Core library working end to end. It consists of a React Frontend and a Backend_For_Frontend (a .NET API) that integrates the Messaging Core library. The user selects a messaging provider, reviews that provider's default settings, and then triggers a "Quote Accepted" event. Three mocked microservices (Sales Leads MS as publisher, Customer MS and Communications MS as consumers) exchange that event through the selected provider, and the Frontend shows the three services as blocks with animated interaction between them.

The microservices contain no real business logic. Their only purpose is to exercise the library's publish and consume paths (publisher role, consumer role, idempotent consumption, correlation id propagation, retries, dead-lettering, observability).

Scope boundary (what this feature covers):
- A React Frontend and a Backend_For_Frontend stored in a new top-level folder (`demo/`) that is separate from the library.
- Provider selection, display of provider default settings, and provider connection start-up.
- Three mocked microservices communicating through Messaging Core over the selected provider.
- Real-time visualisation of the publish and consume flow.

Scope boundary (what this feature does NOT cover):
- Any change to the Messaging Core library source under `src/`, its tests under `tests/`, or its solution file contents.
- Real customer creation, real email delivery, real CRM or sales-lead persistence.
- Authentication and authorisation of demo users (the demo is a local, single-user tool).
- Production deployment, hosting, and scaling of the demo.
- Implementing a new Azure Service Bus transport (see Assumptions).

Assumptions and constraints grounded in the repository:
- **Provider availability.** The library currently has live implementations for RabbitMQ (MassTransit, Publish-Subscribe), Amazon SNS/SQS (MassTransit `AmazonSqs`, verified against LocalStack), and Kafka (native Confluent.Kafka, Streaming mode). The `MassTransitTransport` enum declares `AzureServiceBus`, but no Azure Service Bus bus adapter, settings class, or spec exists in the repository. The demo therefore lists Azure Service Bus as a provider choice that is shown as unavailable, and does not attempt to start it. It becomes selectable when the library delivers that transport.
- **Local infrastructure.** `docker-compose.yml` already provides Kafka (`localhost:9092`) and LocalStack for SNS/SQS (`localhost:4566`). A RabbitMQ broker is not in that compose file, so the demo supplies its own RabbitMQ container definition inside `demo/`.
- **Fan-out semantics.** One Quote Accepted event must reach two different consumers. This maps to Publish-Subscribe on RabbitMQ and SNS/SQS (SNS fan-out to one SQS queue per consumer) and to two distinct consumer groups on Kafka.
- **Secrets.** Demo settings use local throwaway values only (for example LocalStack `test`/`test`). The library's rule that secret-bearing values are never logged or displayed applies to the demo.

## Glossary

- **Demo_App**: The complete demonstration solution located under the `demo/` folder, made up of the Frontend and the Backend_For_Frontend.
- **Frontend**: The React single-page application of the Demo_App that the user interacts with in a browser.
- **Backend_For_Frontend**: The .NET API of the Demo_App that serves the Frontend, hosts the Mock_Microservices, and integrates the Messaging_Core_Library.
- **Messaging_Core_Library**: The existing transport-neutral messaging library in this repository (`Messaging`, `Messaging.Abstractions`, `Messaging.Contracts`, `Messaging.MassTransit`, `Messaging.Kafka`), consumed by the Demo_App as a project or package reference without modification.
- **Provider**: A messaging transport the user can choose: Amazon_SNS_SQS, RabbitMQ, Kafka, or Azure_Service_Bus.
- **Supported_Provider**: A Provider for which the Messaging_Core_Library has a live implementation (Amazon_SNS_SQS, RabbitMQ, Kafka).
- **Unsupported_Provider**: A Provider the Messaging_Core_Library does not yet implement (currently Azure_Service_Bus).
- **Provider_Selector**: The Frontend view where the user chooses a Provider.
- **Provider_Defaults**: The set of non-secret default settings the Demo_App uses for a Provider (for example endpoint, region, topic, queue or consumer group names).
- **Settings_Panel**: The Frontend view that shows the Provider_Defaults after a Provider is selected.
- **Mock_Microservice**: A simulated service with no real business logic. The Demo_App has three: Sales_Leads_MS, Customer_MS, and Communications_MS.
- **Sales_Leads_MS**: The mock publisher microservice that publishes the Quote_Accepted_Event.
- **Customer_MS**: The mock consumer microservice that consumes the Quote_Accepted_Event and creates a Mock_Customer.
- **Communications_MS**: The mock consumer microservice that consumes the Quote_Accepted_Event and sends a Mock_Email.
- **Quote_Accepted_Event**: The message contract published by Sales_Leads_MS, representing one accepted quote and therefore one new Sales_Lead.
- **Sales_Lead**: A mock lead record identified by a Lead_Id and generated for each Quote_Accepted_Event.
- **Mock_Customer**: An in-memory record that Customer_MS creates from a Quote_Accepted_Event. It is not persisted outside the Demo_App process.
- **Mock_Email**: An in-memory record that Communications_MS creates to represent a quote email. No email is transmitted.
- **Trigger_Button**: The Frontend control the user clicks to start a new Quote_Accepted_Event.
- **Flow_Diagram**: The Frontend view that shows the three Mock_Microservices as blocks and the message interaction between them.
- **Flow_Step**: One observable stage of an event's journey: Published, Delivered, Consumed, Customer_Created, Email_Sent, or Failed.
- **Activity_Feed**: The Frontend list of Flow_Steps, ordered by time, each with its Correlation_Id and Lead_Id.
- **Correlation_Id**: The identifier carried in the Messaging_Core_Library `MessageEnvelope` that links all Flow_Steps of one Quote_Accepted_Event.
- **Session**: The period during which one Provider is active in the Backend_For_Frontend, from successful start-up until the user changes Provider or stops it.
- **Provider_Health**: The reachability status of the active Provider as reported by the Messaging_Core_Library transport probe.

## Requirements

### Requirement 1: Isolated Demo Code Location

**User Story:** As a library maintainer, I want the demo code in its own folder, so that the library stays untouched and releasable on its own.

#### Acceptance Criteria

1. THE Demo_App SHALL store all of its source files, configuration, container definitions, and documentation under a single top-level `demo/` folder of the repository, and SHALL create zero files outside `demo/`.
2. THE Demo_App SHALL consume the Messaging_Core_Library through project references or package references only, and SHALL NOT copy any Messaging_Core_Library source file into `demo/`.
3. THE Demo_App SHALL leave every file under `src/` and `tests/` byte-for-byte unchanged, as verified by an empty version-control diff for those two folders after the Demo_App is added.
4. THE Demo_App SHALL leave `MessagingCore.sln`, `global.json`, and `docker-compose.yml` at the repository root byte-for-byte unchanged.
5. WHEN the build command documented in the `demo/` README is run on a clean checkout that has the prerequisites listed in that README, THE Demo_App SHALL build the Backend_For_Frontend and the Frontend with exit code 0 without any modification to `MessagingCore.sln`.
6. IF the Demo_App build fails, THEN THE Demo_App SHALL leave every file outside `demo/` unchanged and SHALL report the failure through a non-zero exit code and build output that names the failing project.
7. THE Demo_App SHALL provide a README at `demo/README.md` that lists the prerequisites with minimum versions, the exact commands to build and start the Demo_App, the exact commands to stop it, and the full URL, including host and port, at which the Frontend is reachable.

### Requirement 2: Provider Selection

**User Story:** As a demo user, I want to choose a messaging provider first, so that I can see the same scenario run over different transports.

#### Acceptance Criteria

1. WHEN the Frontend loads and no Session is active, THE Provider_Selector SHALL display Amazon SNS/SQS, RabbitMQ, Kafka, and Azure Service Bus as the four Provider choices, in that order, within 2 seconds of receiving the Provider list from the Backend_For_Frontend.
2. WHILE no Provider is selected, THE Frontend SHALL hide the Trigger_Button and the Flow_Diagram.
3. WHERE a Provider is an Unsupported_Provider, THE Provider_Selector SHALL mark it as unavailable (not selectable) and SHALL display the text reason that the Messaging_Core_Library has no implementation for it, visible without any user interaction.
4. WHEN the user attempts to select an Unsupported_Provider, THE Provider_Selector SHALL keep the current selection, Settings_Panel contents, and Trigger_Button and Flow_Diagram visibility unchanged, SHALL NOT send a Provider_Defaults request, and SHALL display the unavailability reason.
5. WHEN the user selects a Supported_Provider, THE Frontend SHALL request the Provider_Defaults for that Provider from the Backend_For_Frontend and SHALL display them in the Settings_Panel within 2 seconds of receiving the response.
6. THE Backend_For_Frontend SHALL expose the list of Providers together with a flag that states whether each Provider is supported, and for each Provider with the flag set to unsupported, a reason text of 1 to 200 characters.
7. IF the Provider list request or the Provider_Defaults request fails or receives no response within 10 seconds, THEN THE Frontend SHALL display an error message indicating the failure, SHALL keep the Trigger_Button and Flow_Diagram hidden, SHALL NOT display Provider_Defaults for the failed Provider, and SHALL allow the user to retry the request.
8. WHEN the user selects a different Supported_Provider while a Provider_Defaults request for a previous Provider is pending, THE Frontend SHALL discard the response of the previous request and SHALL display only the Provider_Defaults of the most recently selected Provider.

### Requirement 3: Provider Default Settings Display

**User Story:** As a demo user, I want to see the default settings of the chosen provider before starting, so that I understand how the library is configured.

#### Acceptance Criteria

1. WHEN a Supported_Provider is selected, THE Settings_Panel SHALL display the Provider_Defaults as name and value pairs within 1 second of selection, including the Messaging_Core_Library messaging mode used for that Provider.
2. WHEN Amazon SNS/SQS is selected, THE Settings_Panel SHALL display the `AmazonSqs` settings for Region, ServiceUrl, EndpointName, AutoCreateTopology, RawMessageDelivery, Fifo, MaxReceiveCount, VisibilityTimeoutSeconds, and WaitTimeSeconds, with the defaults defined by the library and the LocalStack service URL `http://localhost:4566`.
3. WHEN RabbitMQ is selected, THE Settings_Panel SHALL display the transport name, the broker host, and the exchange and queue names that each Mock_Microservice uses.
4. WHEN Kafka is selected, THE Settings_Panel SHALL display the bootstrap servers `localhost:9092`, the topic name, the security protocol, and the consumer group identifier of each consuming Mock_Microservice.
5. WHEN a Supported_Provider is selected, THE Settings_Panel SHALL display the library-wide defaults for retry (maximum attempts and backoff), idempotency, and dead-lettering that the Demo_App applies.
6. THE Backend_For_Frontend SHALL replace the value of every secret-bearing setting (credentials, access keys, secret keys, session tokens, passwords) with a fixed masked placeholder before returning Provider_Defaults to the Frontend, so that no secret value appears in any response returned to the Frontend.
7. WHERE the user edits a non-secret value in the Settings_Panel, THE Backend_For_Frontend SHALL validate the edited value through the Messaging_Core_Library configuration validator before using it.
8. IF the Messaging_Core_Library configuration validator rejects an edited value, THEN THE Settings_Panel SHALL display the setting name and the permitted values or range reported by the validator, SHALL keep the previous valid value, and SHALL NOT apply the rejected value to the Demo_App.
9. IF the Backend_For_Frontend cannot retrieve the Provider_Defaults for the selected Supported_Provider, THEN THE Settings_Panel SHALL display an error message indicating that the defaults could not be loaded, SHALL show no partial or stale name and value pairs, and SHALL keep the provider selection unchanged.
10. WHERE the user attempts to edit a secret-bearing setting in the Settings_Panel, THE Settings_Panel SHALL keep the masked placeholder displayed and SHALL NOT submit an edited value for that setting to the Backend_For_Frontend.

### Requirement 4: Provider Start-up

**User Story:** As a demo user, I want to start the selected provider, so that the microservices connect and become ready.

#### Acceptance Criteria

1. THE Settings_Panel SHALL provide a Start control for the selected Supported_Provider.
2. WHEN the user activates the Start control, THE Backend_For_Frontend SHALL validate the configuration, resolve the Provider through the Messaging_Core_Library provider registry, and start the consumer hosts of Customer_MS and Communications_MS and the publisher of Sales_Leads_MS.
3. WHILE the Provider is starting, THE Frontend SHALL display a starting status and SHALL disable the Start control and the Provider_Selector.
4. WHEN all three Mock_Microservices report ready, THE Frontend SHALL display the Flow_Diagram, SHALL enable the Trigger_Button, SHALL display Provider_Health as reachable, and SHALL remove the starting status while keeping the Start control and the Provider_Selector disabled.
5. IF the Provider cannot be reached within 30 seconds of the Start request, THEN THE Backend_For_Frontend SHALL stop all partially started components within 10 seconds, and THE Frontend SHALL display an error that names the Provider and the dependency host and suggests the matching container start command, SHALL keep the Trigger_Button disabled, and SHALL re-enable the Start control and the Provider_Selector.
6. IF the Messaging_Core_Library configuration validation fails at start-up, THEN THE Backend_For_Frontend SHALL NOT start any Mock_Microservice, and THE Frontend SHALL display the name of each failing setting, SHALL keep the Trigger_Button disabled, and SHALL re-enable the Start control and the Provider_Selector.
7. THE Backend_For_Frontend SHALL keep exactly one active Provider per Session.
8. WHEN the user chooses to change Provider during a Session, THE Backend_For_Frontend SHALL drain in-flight messages through the Messaging_Core_Library graceful shutdown within the configured shutdown timeout, SHALL stop all Mock_Microservices, and THE Frontend SHALL disable the Trigger_Button, hide the Flow_Diagram, and return to the Provider_Selector.
9. WHILE a Session is active, THE Frontend SHALL display the active Provider name and the Provider_Health.
10. IF a Mock_Microservice reports a failure to become ready before the 30-second limit, THEN THE Backend_For_Frontend SHALL stop all partially started components within 10 seconds, and THE Frontend SHALL display an error that names the failed Mock_Microservice, SHALL keep the Trigger_Button disabled, and SHALL re-enable the Start control and the Provider_Selector.

### Requirement 5: Mock Microservice Topology

**User Story:** As a demo user, I want to see three separate microservice blocks, so that I understand who publishes and who consumes.

#### Acceptance Criteria

1. WHILE a Session is active, THE Flow_Diagram SHALL display exactly three Mock_Microservice blocks, labelled "Sales Leads MS", "Customer MS", and "Communications MS", and SHALL display no other Mock_Microservice blocks.
2. WHILE a Session is active, THE Flow_Diagram SHALL label Sales Leads MS as "Publisher" and SHALL label Customer MS and Communications MS each as "Consumer".
3. WHILE a Session is active, THE Flow_Diagram SHALL display exactly one broker element, labelled with the name of the active Provider, positioned between the Publisher block and the two Consumer blocks.
4. WHILE a Session is active, THE Flow_Diagram SHALL display exactly three connections: one from Sales Leads MS to the broker element, one from the broker element to Customer MS, and one from the broker element to Communications MS.
5. WHEN a Session starts, THE Backend_For_Frontend SHALL configure Sales_Leads_MS with the publisher role and each of Customer_MS and Communications_MS with the consumer role of the Messaging_Core_Library before the Session is reported as active.
6. IF configuring any Mock_Microservice fails during Session start, THEN THE Backend_For_Frontend SHALL NOT report the Session as active, SHALL release any Mock_Microservice already configured for that Session, and SHALL return an error indication identifying the Mock_Microservice that failed.
7. THE Backend_For_Frontend SHALL give Customer_MS and Communications_MS distinct consumer identities (distinct endpoint or queue names, or distinct consumer group identifiers) so that each receives its own copy of every Quote_Accepted_Event, and a message consumed by one SHALL NOT be removed from delivery to the other.
8. THE Mock_Microservices SHALL interact only through the Messaging_Core_Library `IMessagePublisher` and `IMessageHandler<TContract>` abstractions and SHALL NOT call each other directly.
9. WHEN a Mock_Microservice publishes a Quote_Accepted_Event, creates a customer, or sends an email, THE Flow_Diagram SHALL update the corresponding count on that block within 1 second. The counts are Quote_Accepted_Events published (Sales Leads MS), customers created (Customer MS), and emails sent (Communications MS).
10. WHEN a Session starts, THE Flow_Diagram SHALL display a count of 0 on each Mock_Microservice block. Each count SHALL be a non-negative integer, SHALL only increase during the Session, and SHALL reset to 0 when a new Session starts.

### Requirement 6: Quote Accepted Event Trigger and Publish

**User Story:** As a demo user, I want to click a button to trigger a Quote Accepted event, so that Sales Leads MS publishes it.

#### Acceptance Criteria

1. WHILE a Session is active and all three Mock_Microservices are ready, THE Frontend SHALL display an enabled Trigger_Button labelled "Quote Accepted".
2. WHILE a Session is not active or fewer than three Mock_Microservices are ready, THE Frontend SHALL display the Trigger_Button in a disabled state and SHALL NOT send any trigger request to the Backend_For_Frontend.
3. WHEN the user clicks the enabled Trigger_Button, THE Backend_For_Frontend SHALL instruct Sales_Leads_MS to create one new Sales_Lead with a unique Lead_Id and to publish one Quote_Accepted_Event for that Sales_Lead through `IMessagePublisher`.
4. WHEN Sales_Leads_MS publishes a Quote_Accepted_Event, THE Sales_Leads_MS SHALL populate the `MessageEnvelope` with a message identifier unique across all events in the Session, a Correlation_Id of 1 to 128 non-whitespace characters, the contract type name of Quote_Accepted_Event, and a UTC timestamp set within 1 second of the publish call.
5. THE Quote_Accepted_Event payload SHALL contain the Lead_Id, a generated mock customer name of 1 to 100 characters, a generated mock customer email address of 3 to 254 characters in a valid email format, a generated mock quote amount between 0.01 and 999,999.99 with two decimal places, and the UTC time of acceptance.
6. WHEN `IMessagePublisher` returns a successful result, THE Frontend SHALL show a Published Flow_Step for that Correlation_Id in the Activity_Feed within 1 second of the result being returned.
7. IF `IMessagePublisher` returns a failed result, THEN THE Frontend SHALL show a Failed Flow_Step for that Correlation_Id in the Activity_Feed within 1 second, including the failure reason reported by the Messaging_Core_Library, THE Frontend SHALL keep the Trigger_Button enabled, and THE Backend_For_Frontend SHALL NOT retain the Sales_Lead as published.
8. IF the Backend_For_Frontend cannot reach Sales_Leads_MS or receives no response within 10 seconds of the click, THEN THE Frontend SHALL show an error indication that the trigger failed, SHALL keep the Trigger_Button enabled, and SHALL NOT add a Published Flow_Step for that click.
9. WHEN the user clicks the Trigger_Button more than once, THE Backend_For_Frontend SHALL publish exactly one separate Quote_Accepted_Event, with a distinct Lead_Id and a distinct Correlation_Id, for each click.
10. WHILE one or more earlier Quote_Accepted_Events are in flight, THE Frontend SHALL accept each click on the Trigger_Button and SHALL keep the Trigger_Button enabled.

### Requirement 7: Customer Microservice Consumption

**User Story:** As a demo user, I want Customer MS to create a customer when it receives the event, so that I can see a lead become a customer.

#### Acceptance Criteria

1. WHEN Customer_MS receives a Quote_Accepted_Event, THE Customer_MS SHALL bind the payload to the Quote_Accepted_Event contract through the Messaging_Core_Library and SHALL invoke its `IMessageHandler<TContract>` implementation exactly once for that delivery.
2. WHEN the Customer_MS handler runs for a Quote_Accepted_Event whose Lead_Id has no existing Mock_Customer, THE Customer_MS SHALL create one Mock_Customer that carries the Lead_Id, name, and email address from the Quote_Accepted_Event, with each value identical to the event value.
3. WHEN the Customer_MS handler completes successfully, THE Frontend SHALL show a Customer_Created Flow_Step within 5 seconds that carries the same Correlation_Id and Lead_Id as the Published Flow_Step.
4. WHEN Customer_MS receives a Quote_Accepted_Event whose idempotency key it has already processed, THE Customer_MS SHALL create no additional Mock_Customer, SHALL leave the existing Mock_Customer unchanged, and SHALL acknowledge the message as processed.
5. THE Customer_MS SHALL create at most one Mock_Customer per Lead_Id.
6. IF Customer_MS receives a Quote_Accepted_Event whose payload cannot be bound to the Quote_Accepted_Event contract, or whose Lead_Id, name, or email address is missing or empty, THEN THE Customer_MS SHALL create no Mock_Customer, SHALL NOT invoke the handler for the invalid payload, and SHALL route the message according to the Messaging_Core_Library failure handling (retry or dead-letter).
7. IF the Customer_MS handler fails before completing, THEN THE Customer_MS SHALL leave no partially created Mock_Customer, SHALL NOT show a Customer_Created Flow_Step, and SHALL allow the Messaging_Core_Library to retry the message.
8. IF Customer_MS receives a Quote_Accepted_Event with a new idempotency key but a Lead_Id that already has a Mock_Customer, THEN THE Customer_MS SHALL create no additional Mock_Customer and SHALL leave the existing Mock_Customer unchanged.

### Requirement 8: Communications Microservice Consumption

**User Story:** As a demo user, I want Communications MS to send a quote email when it receives the event, so that I can see the second consumer react to the same event.

#### Acceptance Criteria

1. WHEN Communications_MS receives a Quote_Accepted_Event, THE Communications_MS SHALL bind the payload to the Quote_Accepted_Event contract through the Messaging_Core_Library and SHALL invoke its `IMessageHandler<TContract>` implementation exactly once for that delivery.
2. WHEN the Communications_MS handler runs, THE Communications_MS SHALL create exactly one Mock_Email addressed to the email address in the Quote_Accepted_Event, with a non-empty subject of at most 200 characters and a non-empty body of at most 5,000 characters, where both the subject and the body contain the quote amount from the Quote_Accepted_Event.
3. WHEN the Communications_MS handler completes successfully, THE Frontend SHALL show an Email_Sent Flow_Step that carries the same Correlation_Id and Lead_Id as the Published Flow_Step within 2 seconds of handler completion.
4. IF Communications_MS receives a Quote_Accepted_Event whose idempotency key it has already processed, THEN THE Communications_MS SHALL create no additional Mock_Email, SHALL add no additional Email_Sent Flow_Step, and SHALL acknowledge the message as successfully handled.
5. THE Communications_MS SHALL transmit no network email, and THE Mock_Email SHALL exist only in the memory of the Backend_For_Frontend process and SHALL NOT persist after that process restarts.
6. IF the Communications_MS handler fails while processing a Quote_Accepted_Event, THEN THE Communications_MS SHALL create no Mock_Email for that attempt, SHALL show no Email_Sent Flow_Step for that attempt, and SHALL apply the Messaging_Core_Library retry and dead-letter policy to that message.
7. THE Customer_MS and the Communications_MS SHALL process each Quote_Accepted_Event independently, so that a failure, delay, or dead-lettering in one does not prevent, delay, or alter the outcome of the other processing the same event.

### Requirement 9: Interaction Visualisation

**User Story:** As a demo user, I want the blocks to animate as messages move, so that I can follow the interaction between microservices.

#### Acceptance Criteria

1. WHEN a Published Flow_Step occurs, THE Flow_Diagram SHALL animate a message marker from the Sales Leads MS block to the broker element, starting within 500 milliseconds of the Frontend receiving the Flow_Step and lasting between 500 and 2000 milliseconds.
2. WHEN a Delivered Flow_Step occurs for a Consumer, THE Flow_Diagram SHALL animate a message marker from the broker element to that Consumer block, starting within 500 milliseconds of the Frontend receiving the Flow_Step and lasting between 500 and 2000 milliseconds.
3. WHEN a Customer_Created Flow_Step occurs, THE Flow_Diagram SHALL highlight the Customer MS block for 2 seconds and SHALL show a Mock_Customer indicator that carries the Lead_Id.
4. WHEN an Email_Sent Flow_Step occurs, THE Flow_Diagram SHALL highlight the Communications MS block for 2 seconds and SHALL show a Mock_Email indicator that carries the Lead_Id.
5. WHEN a Flow_Step occurs, THE Backend_For_Frontend SHALL push the Flow_Step to the Frontend within 500 milliseconds of the Flow_Step occurring, without the Frontend polling for it.
6. WHEN a Flow_Step is received by the Frontend, THE Activity_Feed SHALL add an entry for it, ordered by UTC timestamp with the newest entry first, showing the UTC timestamp, the originating Mock_Microservice, the Correlation_Id, and the Lead_Id, and SHALL retain at most 500 entries by discarding the oldest first.
7. WHEN the user selects a Correlation_Id in the Activity_Feed, THE Frontend SHALL highlight every Flow_Step in the Activity_Feed that carries that Correlation_Id and SHALL remove the highlight from Flow_Steps that carry a different Correlation_Id.
8. WHEN the Published, Customer_Created, and Email_Sent Flow_Steps have all occurred for one Quote_Accepted_Event, THE Frontend SHALL mark that event as completed in the Activity_Feed within 500 milliseconds of the last of those Flow_Steps being received.
9. WHEN the user activates the clear control, THE Frontend SHALL remove all entries from the Activity_Feed, SHALL reset all displayed counters to zero, and SHALL keep the Session active.
10. WHEN the Frontend reconnects to the Backend_For_Frontend during an active Session, THE Backend_For_Frontend SHALL replay all Flow_Steps of the Session that occurred after the last clear action, in their original order, so that the Frontend shows the same Activity_Feed as before the disconnect, and SHALL complete the replay within 5 seconds of reconnection.
11. IF the Frontend loses its connection to the Backend_For_Frontend, THEN THE Frontend SHALL display a disconnected indicator within 2 seconds, SHALL retain the existing Activity_Feed entries, and SHALL attempt to reconnect at intervals of 1 to 10 seconds until the connection is restored or the Session ends.
12. IF a Flow_Step is received whose Correlation_Id or Lead_Id is missing, THEN THE Frontend SHALL still add the Flow_Step to the Activity_Feed with the missing field shown as empty, and SHALL not stop animating subsequent Flow_Steps.

### Requirement 10: Failure and Resilience Demonstration

**User Story:** As a demo user, I want to see what happens when a consumer fails, so that I can see the library's retry and dead-letter behaviour.

#### Acceptance Criteria

1. WHERE failure simulation is set to "Fail once" for a Consumer, THE Backend_For_Frontend SHALL make that Consumer's handler throw a transient failure on delivery attempt 1 of each Quote_Accepted_Event and succeed on delivery attempt 2 of the same Quote_Accepted_Event.
2. WHERE failure simulation is set to "Always fail" for a Consumer, THE Backend_For_Frontend SHALL make that Consumer's handler throw a non-transient failure on every delivery attempt of every Quote_Accepted_Event.
3. WHEN the Messaging_Core_Library schedules a retry of a delivery, THE Frontend SHALL show a Retrying Flow_Step within 2 seconds, and that Flow_Step SHALL display the attempt number (an integer from 1 up to the configured maximum retry attempts) and the Correlation_Id.
4. WHEN the Messaging_Core_Library routes a message to the dead-letter sink, THE Frontend SHALL show a Dead_Lettered Flow_Step within 2 seconds, and that Flow_Step SHALL display the Correlation_Id and the failure kind ("transient" or "non-transient").
5. THE Frontend SHALL provide, for each Consumer, a toggle with exactly three options (Off, Fail once, and Always fail), with Off selected by default.
6. WHEN a Consumer's failure simulation setting is changed, THE Backend_For_Frontend SHALL apply the new setting to Quote_Accepted_Events first delivered after the change, and SHALL leave deliveries already in progress unchanged.
7. WHEN one Consumer fails or dead-letters a Quote_Accepted_Event, THE other Consumer SHALL still process that same Quote_Accepted_Event, and THE Frontend SHALL show that Consumer's success Flow_Step for the event within 5 seconds of the event being published.
8. IF the Frontend cannot save a failure simulation setting change to the Backend_For_Frontend, THEN THE Frontend SHALL show an error message indicating the setting was not saved and SHALL keep the toggle at its previously saved option.

### Requirement 11: Observability and Library Integration Evidence

**User Story:** As a developer evaluating the library, I want to see which library features the demo uses, so that I can reproduce the integration in my own service.

#### Acceptance Criteria

1. WHEN the Backend_For_Frontend starts, THE Backend_For_Frontend SHALL register the Messaging_Core_Library through its dependency-injection extension methods and the provider-specific registration of the selected Provider.
2. IF the Messaging_Core_Library registration or the provider-specific registration fails during startup, THEN THE Backend_For_Frontend SHALL terminate startup, write a log entry identifying the failed registration, and not accept API requests.
3. WHEN a liveness or readiness request is received, THE Backend_For_Frontend SHALL return the result of the corresponding Messaging_Core_Library health check within 2 seconds.
4. IF the readiness health check reports the Provider as unavailable, THEN THE Backend_For_Frontend SHALL report a not-ready result to the caller while the liveness endpoint continues to report the process as alive.
5. WHEN a publish, consume, retry, or dead-letter operation occurs, THE Backend_For_Frontend SHALL write a structured log entry that includes the Correlation_Id, the operation type, and the message type.
6. THE Backend_For_Frontend SHALL exclude message payload contents, credentials, connection strings, and tokens from all log entries.
7. WHEN the user opens the integration details view, THE Frontend SHALL display the active messaging mode, the active Provider name, the idempotency setting (enabled or disabled), and the retry setting (maximum attempts and backoff strategy) as reported by the Backend_For_Frontend.
8. IF the Backend_For_Frontend does not return the integration details within 5 seconds or returns an error, THEN THE Frontend SHALL display an error indication that the details are unavailable and SHALL NOT display stale or default values.
9. WHEN the Backend_For_Frontend sends a response to an API request related to a Quote_Accepted_Event, THE Backend_For_Frontend SHALL include the Correlation_Id of that event in the `X-Correlation-Id` response header, for both success and error responses.

### Requirement 12: Backend_For_Frontend API Contract

**User Story:** As a frontend developer, I want a consistent, typed API, so that the Frontend integrates simply and errors are predictable.

#### Acceptance Criteria

1. THE Backend_For_Frontend SHALL expose versioned HTTP endpoints for listing Providers, retrieving Provider_Defaults, starting a Session, stopping a Session, triggering a Quote_Accepted_Event, setting a failure simulation mode, and retrieving the Session status, where the version identifier is part of every endpoint's route.
2. THE Backend_For_Frontend SHALL return typed DTOs and SHALL NOT serialise domain entities or Messaging_Core_Library internal types over the wire.
3. IF a request fails validation (including a missing required field, a field value outside its documented bounds, a malformed body, or an unsupported failure simulation mode), THEN THE Backend_For_Frontend SHALL return an RFC 7807 Problem Details response with HTTP status 400 that lists the name of every invalid field, and SHALL leave the Session state unchanged.
4. IF an unexpected error occurs, THEN THE Backend_For_Frontend SHALL return an RFC 7807 Problem Details response with HTTP status 500 that contains no stack trace, no connection string, and no infrastructure detail, and SHALL include a correlation identifier that the caller can quote to locate the failure.
5. IF the user triggers a Quote_Accepted_Event while no Session is active, THEN THE Backend_For_Frontend SHALL return HTTP status 409 with a Problem Details body indicating that no Provider is started, and SHALL NOT publish any message.
6. IF the user starts a Session while another Session is active, THEN THE Backend_For_Frontend SHALL return HTTP status 409 with a Problem Details body that names the active Provider, and SHALL keep the active Session running unchanged.
7. THE Backend_For_Frontend SHALL allow cross-origin requests only from the configured Frontend origin, and SHALL omit cross-origin permission headers from responses to requests from any other origin.
8. IF a request body contains a field not defined in the request DTO, THEN THE Backend_For_Frontend SHALL reject the request with an RFC 7807 Problem Details response with HTTP status 400 that names the unknown field, and SHALL leave the Session state unchanged.
9. WHEN the Frontend requests an endpoint that does not exist or uses an unsupported HTTP method for an existing endpoint, THE Backend_For_Frontend SHALL return an RFC 7807 Problem Details response with HTTP status 404 or 405 respectively.
10. IF a request body exceeds 1 MiB, THEN THE Backend_For_Frontend SHALL reject the request with an RFC 7807 Problem Details response with HTTP status 413 without processing the body.

### Requirement 13: Local Infrastructure and Developer Experience

**User Story:** As a demo user, I want to start the demo with a few commands, so that I can show it without manual broker setup.

#### Acceptance Criteria

1. THE Demo_App SHALL provide a container definition under `demo/` that starts RabbitMQ with a pinned release image tag.
2. THE Demo_App SHALL reuse the Kafka and LocalStack services defined in the repository `docker-compose.yml` without modifying that file.
3. THE Demo_App SHALL document, for each Supported_Provider, the single command that starts the required broker container.
4. THE Demo_App SHALL reference every container image by an explicit fixed-release version tag and SHALL NOT reference a floating `latest` tag.
5. THE Demo_App SHALL load all connection values and credentials from environment variables or local configuration that contains no real secret.
6. THE Frontend SHALL read the Backend_For_Frontend base URL from its build or runtime configuration.
7. WHEN the documented start command for a Supported_Provider is run on a host with Docker running and the required ports free, THE Demo_App SHALL start the broker container for that provider and report it as healthy within 60 seconds.
8. IF the documented start command for a Supported_Provider fails because Docker is not running or a required port is already in use, THEN THE Demo_App SHALL exit with a non-zero exit code and print an error message that names the failing container or port, and SHALL NOT leave a partially started broker container running.
9. IF a required environment variable or configuration value for a connection or credential is missing or empty at startup, THEN THE Demo_App SHALL refuse to start, exit with a non-zero exit code, and print an error message that names each missing setting.
10. IF the Backend_For_Frontend base URL is missing or is not a valid absolute URL, THEN THE Frontend SHALL display an error message indicating the configuration problem and SHALL NOT send any request to the Backend_For_Frontend.

### Requirement 14: Quality and Verification

**User Story:** As a maintainer, I want the demo covered by automated tests, so that it stays working as the library evolves.

#### Acceptance Criteria

1. THE Demo_App SHALL include unit tests that cover every command handler and every query handler of the Backend_For_Frontend, every handler of the Mock_Microservice services, the secret-masking function, and the Provider_Defaults construction for each Supported_Provider, with at least one passing test per handler and per Supported_Provider.
2. WHEN the integration test runs against a real containerised broker and publishes one Quote_Accepted_Event through Sales_Leads_MS, THE Demo_App test suite SHALL verify within 30 seconds that Customer_MS holds exactly one Mock_Customer and Communications_MS holds exactly one Mock_Email for that Lead_Id.
3. THE Demo_App SHALL include Frontend component tests for the Provider_Selector, the Settings_Panel, the Trigger_Button, and the Activity_Feed, with at least one passing test per component.
4. THE Demo_App Frontend component tests for the Provider_Selector SHALL verify that an Unsupported_Provider is rendered in the unavailable state and cannot be selected.
5. FOR ALL Quote_Accepted_Event payloads, serialising then binding the payload through the Messaging_Core_Library contract binder SHALL produce a payload equal to the original in every field (round-trip property).
6. FOR ALL secret-bearing setting values, THE masking function SHALL return output that does not contain the original value as a substring (including empty-string, 1-character, and 256-character inputs).
7. FOR ALL sequences of N Quote_Accepted_Event triggers (1 ≤ N ≤ 100) followed by delivery of all messages, THE Customer_MS SHALL hold exactly N Mock_Customers and THE Communications_MS SHALL hold exactly N Mock_Emails, each with a distinct Lead_Id (invariant).
8. FOR ALL Quote_Accepted_Event messages delivered 2 to 10 times, THE Customer_MS and THE Communications_MS SHALL hold the same Mock_Customer and Mock_Email counts as after a single delivery (idempotence).
9. IF an integration test cannot start the containerised broker or the broker is not reachable within 60 seconds, THEN THE Demo_App test suite SHALL report that test as failed with an error indicating the broker was unavailable, and SHALL NOT report it as passed or skipped.
