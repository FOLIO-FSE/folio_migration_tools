# Appendix: mod-orders validation rules for composite orders

This appendix summarizes the validation that FOLIO's `mod-orders` applies when a composite purchase order is POSTed. It was compiled from the `mod-orders` source (`PoLineValidationService`, `OrderValidationService`, `PurchaseOrderHelper`, `PurchaseOrderLineHelper`, `FundDistributionUtils`, `ExpenseClassValidationService`) on the master branch. Rules can change between FOLIO releases, so check the source for your release before relying on a specific rule.

The OrdersTransformer cannot reproduce every rule, because some depend on tenant configuration, budgets and inventory state. The list below shows what a migration should get right in the data itself.

## Which rules apply to Pending orders

`PoLineValidationService.validatePoLine` runs on every POST, whatever the workflow status. A Pending order therefore has to satisfy all the "Always" rules. The "Open only" rules are checked when the order transitions to Open.

## Always enforced (including Pending)

### Cost quantities by order format

| Order format | Physical cost quantity | Electronic cost quantity |
| --- | --- | --- |
| Physical Resource | must be greater than 0 | must be 0 |
| Other | must be greater than 0 | must be 0 |
| Electronic Resource | must be 0 | must be greater than 0 |
| P/E Mix | must be greater than 0 | must be greater than 0 |

A line with zero physical and electronic cost quantities and no locations skips the checks from this point on.

### Locations

* The sum of physical quantities across locations must equal the physical cost quantity. The same applies to electronic quantities. The check applies when the line's inventory interaction creates holdings for that format, or when any location has a quantity above 0 for it.
* Every location must have a total quantity (physical plus electronic) above 0.
* Every location must have exactly one of `locationId` or `holdingId`.
* `location.quantity` is recomputed by mod-orders as `quantityPhysical + quantityElectronic`. Only the physical and electronic totals are compared with the cost.

### Cost prices

* Electronic Resource: `listUnitPrice` must not be above 0.
* Other formats: `listUnitPrice` must not be negative.
* Electronic Resource and P/E Mix: `listUnitPriceElectronic` must not be negative. Other formats: it must not be above 0.
* `additionalCost` must not be negative.
* `discount` must not be negative. A percentage discount must not exceed 100. An amount discount must not make the estimated price negative.

### Other line rules

* A package line (`isPackage`) must not have an `instanceId`.
* Claiming: `claimingInterval` must be above 0 when `claimingActive` is true.
* Binder active (`details.isBinderyActive`): order format must be Physical Resource or P/E Mix, inventory interaction must create items, and the receiving workflow must be Independent (`checkinItems` true, which is not the default).
* Receipt not required (`receiptStatus`): the receiving workflow must be Independent (`checkinItems` true).
* Prepayment term: for a line with `multiYearPayment`, a `paymentTerms.prepaymentTerm` above 0 requires that many `fiscalYearDistributions` (`FundDistributionUtils.validatePrepaymentTerm`).
* Expense class: a fund distribution's expense class must exist in the fund's budget.
* Order level: an Ongoing order requires the `ongoing` object (mod-orders rejects it without one) and a One-Time order must not have one. The number of lines must not exceed the tenant's limit.
* PO number must be unique, and the prefix and suffix must exist.

## Open only

* Material type is required on any POL whose inventory interaction is Instance, Holding, Item (`MISSING_MATERIAL_TYPE`). An empty material type is therefore valid for Pending orders and for lines that do not create items.
* Fund distribution percentages or amounts must add up to the estimated price.
* Vendor and access provider must be active organizations.
* Funds, budgets and expense classes must be active.
* The order must have at least one line, checked when an existing order is changed to Open.

## Inventory interaction defaults

When a POL leaves `createInventory` null, mod-orders fills it from the tenant's orders settings:

| Format | Default if the tenant has no setting |
| --- | --- |
| Physical Resource, Other | Instance, Holding, Item |
| Electronic Resource | Instance, Holding |

The default is applied only when the value is null, so an explicit value is never changed. Because a defaulted line can become Instance, Holding, Item and then needs a material type at Open, a migration that leaves `createInventory` empty should read the tenant setting first to know whether material type and locations are required.

The OrdersTransformer reads the tenant setting (`/orders-storage/settings`, key `createInventory`) for this reason. If the setting is missing or cannot be read, it assumes `None` for every format instead of the mod-orders defaults above. Its validation is then looser than FOLIO's: a line that FOLIO would default to Instance, Holding, Item is not checked for a material type or for location quantities. The log shows a warning when the read fails.

## What the OrdersTransformer checks

The transformer fails a record before it reaches FOLIO when it breaks one of these rules:

* Cost quantities by order format, location quantity totals against the cost, location quantities above 0, and exactly one of `locationId` or `holdingId` per location.
* Cost prices: list unit prices, additional cost, and the discount sign and percentage limit. An amount discount that makes the estimated price negative is also rejected.
* A package line with an `instanceId`.
* Claiming: `claimingInterval` above 0 when `claimingActive` is true.
* Receiving workflow: `checkinItems` must be true (Independent) when `receiptStatus` is Receipt Not Required, and when binder active is set. FOLIO's default for `checkinItems` is false, so an unmapped value counts as false.
* Binder active: order format and inventory interaction.
* Prepayment term: for a line with `multiYearPayment`, the number of `fiscalYearDistributions` must equal `paymentTerms.prepaymentTerm`.
* Order level: if an Ongoing order has no `ongoing` object mapped, the transformer creates a minimal one (`isSubscription` and `manualRenewal` false, the schema defaults). It removes any `ongoing` object from a One-Time order.
* Open orders only: material type on any line whose inventory interaction creates items, and fund distributions that add up to the estimated price (the transformer computes the estimated price from the cost, as mod-orders does).

Checks that read FOLIO data. They run for each fund distribution (funds are read once and cached). If a fund or the setting cannot be read, a warning is logged and the check is skipped:

* Expense class: it must be in an Active budget of the fund (any status), and must not be inactive there when the order is Open. mod-orders looks at the fund's Active budgets in the same way.
* Fund location restrictions (Open orders only): when a fund is restricted by locations and has locations, at least one of them must be among the line's locations. A line with a `holdingId` location is not checked, because the holding's location is not known.
* Vendor and access providers (Open orders only): the order's vendor must be an Active organization flagged as a vendor, and each line's e-resource access provider must be an Active organization. Organizations are read once at startup. The vendor is already matched by code, so an unknown vendor fails the record whatever the workflow status.
* PO line limit: the number of lines must not exceed the tenant setting `poLines-limit` (`/orders-storage/settings`). It is not checked when the setting is missing.

It does not check the following, so they can still fail when the order is loaded:

* PO number uniqueness, and prefix and suffix.
* The other Open-only rules that need FOLIO data: active funds and budgets.
* An Open order with no lines. mod-orders only checks this when an existing order is changed to Open.

## Reference

* Source: <https://github.com/folio-org/mod-orders>, `src/main/java/org/folio/`
