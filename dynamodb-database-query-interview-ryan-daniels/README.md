# DynamoDB Database Query Interview

Hey there applicant! Thanks for stopping by. We are so excited that you want to work for ChannelApe. We just know that you are going to love it here. Anyway enough water cooler banter, it's time to get down to business! We need you to create a service that generates a report of completed tasks our awesome company has automated for our users. You should find everything you need to get started in the documentation below. Good luck!

## Requirements

`ReportGenerationService` needs to query the ChannelApe database for task information filtering based on task completion date and task target then output a valid [RF4180 CSV file](https://tools.ietf.org/html/rfc4180). Please submit your work in the form of a pull request.

## ChannelApe Database Architecture

### Businesses

An organization within ChannelApe with one or many users. A business is associated with a usage based subscription. The subscription is what is paid each month by the owner of the business.

#### Relevant files
* Mock Data - `src\test\resources\Businesses.csv`
* DynamoDB Class - `DataBusiness.java`

### CompletedTasks

Contains records of tasks that ChannelApe has automated for its users. Completed tasks are always within the scope of a business. In addition to reporting they are also used to charge customers within usage based subscriptions.

#### Relevant files
* Mock Data - `src\test\resources\CompletedTasks.csv`
* DynamoDB Class - `DataCompletedTask.java`

### Integrations

Contains all integrations that ChannelApe supports. Integrations are either of type `channel` (Shopify, Walmart, BigCommerce, etc.) or `supplier` (Europa Sports, Generic CSV File, Customer ERP, etc.). This table is not associated with any businesses. If a Business wishes to install one of the integrations then an entry in the Channels or Businesses table is created associating the business with the integration.

#### Relevant files
* Mock Data - `src\test\resources\Integrations.csv`
* DynamoDB Class - `DataIntegration.java`

### Channels

Contains all `channel` type integrations that have been installed on a business. The `integrationId` links the channel to an integration within the Integrations table. The `businessId` links the channel to a business within the Businesses table.

#### Relevant files
* Mock Data - `src\test\resources\Channels.csv`
* DynamoDB Class - `DataChannel.java`

### Suppliers

Contains all `supplier` type integrations that have been installed on a business. The `integrationId` links the supplier to an integration within the Integrations table. The `businessId` links the supplier to a business within the Businesses table.

#### Relevant files
* Mock Data - `src\test\resources\Suppliers.csv`
* DynamoDB Class - `DataSupplier.java`

## Building from source

	1. Install Maven
	2. Install JDK 8
	3. Clone the repository.
	3. Navigate to repository directory and run `mvn install`

## Testing Your Implementation

Run `mvn install`. If you see a `BUILD SUCCESS` message then you have passed pending code review.

## Bonus

This section is optional but still good to at least look into.

### Mutation Coverage

Achieve a mutation coverage score of 60% or greater on your code base. The [pitest mutation coverage engine](http://pitest.org/quickstart/maven/) is already setup to run as part of the `integration-test` phase of the `mvn install`. Change the `pitest.mutationthreshold` property in the `pom` to 60 and see if `mvn install` still results in a successful build. `pitest` generates an HTML report in the `target/pit-reports` folder which you can use to track your progress. Just open the `index.html` file in your browser.   

### Questions

	1. How would you change the DynamoDB table structure to improve the runtime of the report?
	2. Can you think of any other parameters that users may want to query with? 
	3. Can you think of any other unit tests that would be useful?

