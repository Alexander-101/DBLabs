@file:OptIn(ExperimentalUuidApi::class)

package aab.db4

import net.datafaker.Faker
import org.intellij.lang.annotations.Language
import org.postgresql.util.PGobject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.time.LocalDate
import java.time.Month
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import kotlin.math.absoluteValue
import kotlin.random.Random
import kotlin.random.asJavaRandom
import kotlin.uuid.ExperimentalUuidApi

// Table scaling factors
object TablesScales {
    val roles = 1
    val permissions = 5
    val users = 500
    val admins = 100
    val technicians = 50
    val customers = 200
    val actionsLog = 1000
    val products = 500
    val alcohol = 250
    val snacks = 250
    val conversationTopics = 300
    val promotions = 200
    val orders = 500
    val orderProducts = 1000
    val ordersTopics = 750
    val snackRecommendations = 500
    val stores = 200
    val storeLots = 400
    val promotionProducts = 200
    val promotionsTopics = 100
    val serviceLog = 500
    val droids = 100
}

fun main() {
    val randomSeed = 0
    val baseSize = 1000
    val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
    val user = "user"
    val password = "password"

    val connection = DriverManager.getConnection(jdbcUrl, user, password)
    val random = Random(randomSeed)
    val generator = DataGenerator(connection, random, baseSize)
    generator.generateAll()
}



class DataGenerator(
    private val connection: Connection,
    private val random: Random,
    private val baseSize: Int,
    private val reinit: Boolean = true
) {
    private val faker = Faker(random.asJavaRandom())

    // Enums
    val statusEnum = listOf("SCHEDULED", "IN_PROGRESS", "COMPLETE", "CANCELED", "SUSPENDED")
    val typeEnum = listOf("COMMON", "SPECIAL_MONTHLY_PROMO")
    val sexEnum = listOf("MALE", "FEMALE")

    val actions = listOf("Add", "Remove", "Execute", "Access")

    val alcoholsTypes = listOf("Vodka", "Beer", "Gin", "Rom", "Whiskey", "Tequila", "Brandie", "Spirits")

    // ids for some tables
    val roles = genDistinct(baseSize * TablesScales.roles) { faker.word().noun() }
    val permissions = genDistinct(baseSize * TablesScales.permissions) { "${faker.word().verb()}.${faker.word().verb()}" }

    val topicsNames = listOf("politic", "alcohol", "cars", "games", "football", "vodka", "fishing", "economic", "life")
    val topics = random.genUuids(topicsNames.size)

    //    val roles = random.genStrings(baseSize * TablesScales.roles)
    val users = random.genUuids(baseSize * TablesScales.users)
    val admins = random.chooseFrom(baseSize * TablesScales.admins, users).toSet().toList()
    val technicians = random.chooseFrom(baseSize * TablesScales.technicians, users).toSet().toList()
    val customers = random.chooseFrom(baseSize * TablesScales.customers, users).toSet().toList()
    val dorids = random.genUuids(baseSize * TablesScales.droids)
    val products = random.genUuids(baseSize * TablesScales.products)
    val snacks = random.chooseFrom(baseSize * TablesScales.snacks, products).toSet().toList()
    val alcohols = (products.toSet() - snacks.toSet()).toList()
    val orders = random.genUuids(baseSize * TablesScales.orders)
    val stores = random.genUuids(baseSize * TablesScales.stores)
    val promotions = random.genUuids(baseSize * TablesScales.promotions)


    /**
     * Batch insert n rows
     * @param sql row insert sql
     * @param n amount of rows to insert
     * @param binder function (rowIndex, preparesStatement) -> Unit that bind PreparesStatement parameters for each row
     */
    private fun insert(@Language("PostgreSQL") sql: String, n: Int, binder: (Int, PreparedStatement) -> Unit) {
        println("Inserting ${n} rows")
        connection.prepareStatement(sql).use { ps ->
            for (i in 0..<n) {
                binder(i, ps)
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    /**
     * Batch insert of rows with ids from given list
     * @param sql row insert sql
     * @param ids list of ids to insert
     * @param binder function (rowIndex, id, preparesStatement) -> Unit that bind PreparesStatement parameters for each row
     */
    private fun <T> insert(@Language("PostgreSQL") sql: String, ids: List<T>, binder: (Int, T, PreparedStatement) -> Unit) {
        insert(sql, ids.size) { i, ps ->
            binder(i, ids[i], ps)
        }
    }


    private fun executeSql(sql: String) {
        connection.createStatement().use { stmt ->
            stmt.execute(sql)
        }
    }

    private fun executeInitSql() {
        val initSqlPath = java.nio.file.Paths.get("init.sql")
        val sqlContent = java.nio.file.Files.readString(initSqlPath)
        executeSql(sqlContent)
        println(sqlContent)
    }

    fun generateAll() {
        if (reinit) {
            executeInitSql()
        }
        generateRoles()
        println("Roles done")
        generatePermissions()
        println("Permission done")
        generateUsers()
        println("Users done")
        generateAdmins()
        println("Admins done")
        generateTechnicians()
        println("Technicians done")
        generateCustomers()
        println("Customers done")
        generateActionsLog()
        println("Actions log done")
        generateDroids()
        println("Droids done")
        generateProducts()
        println("Products done")
        generateAlcohol()
        println("Alcohols done")
        generateSnacks()
        println("Snacks done")
        generateConversationTopics()
        println("Conversation topics done")
        generatePromotions()
        println("Promotions done")
        generateStores()
        println("Stores done")
        generateStoreLots()
        println("Store lots done")
        generateOrders()
        println("Orders done")
        generateOrderDroidsAndServiceLog()
        println("Order droids and service log done")
        generateOrderProducts()
        println("Order products done")
        generateOrdersTopics()
        println("Orders topics done")
        generateSnackRecommendations()
        println("Snack recommendations done")
        generatePromotionProducts()
        println("Promotion products done")
        generatePromotionsTopics()
        println("Promotion topics done")
    }

    private fun generateRoles() {
        insert("insert into data.roles (role_id) values (?);", roles) { i, id, ps ->
            ps.setString(1, id)
        }
    }

    private fun generatePermissions() {
        insert("insert into data.permissions (role_id, permission) values (?, ?);", baseSize * TablesScales.permissions) { i, ps ->
            ps.setString(1, random.choose(roles))
            ps.setString(2, permissions[i % permissions.size])
        }
    }

    private fun generateUsers() {
        insert("insert into data.users (id, role_id) values (?, ?);", users) { i, id, ps ->
            ps.setObject(1, id)
            ps.setString(2, random.choose(roles))
        }
    }

    private fun generateAdmins() {
        insert("insert into data.admins (id) values (?);", admins) { i, id, ps ->
            ps.setObject(1, id)
        }
    }

    private fun generateTechnicians() {
        insert("insert into data.technicians (id, base_salary) values (?, ?);", technicians) { i, id, ps ->
            ps.setObject(1, id)
            ps.setDouble(2, random.nextDouble(1000.0, 10000.0))
        }
    }

    private fun generateCustomers() {
        insert("insert into data.customers (id, name, experience_since, birth_date, personal_info) values (?, ?, ?, ?, ?::jsonb);", customers) { i, id, ps ->
            ps.setObject(1, id)
            ps.setString(2, faker.name().nameWithMiddle())
            ps.setObject(3, LocalDate.now().minusDays(random.nextLong(0, 365 * 10)))
            ps.setObject(4, java.sql.Date(random.nextLong(-781770665000, 890760535000)))
            ps.setString(5, """{
                ${if (random.nextBoolean()) """
                    "passport": {
                        "series": ${random.nextInt(0, 10000)},
                        "number": ${random.nextInt(0, 1000000)}
                    }
                """.trimIndent() else """
                    "external_passport": "${faker.passport().valid()}"
                """.trimIndent()}
            }""".trimIndent())
        }
    }

    private fun generateActionsLog() {
        insert("insert into data.actions_log (user_id, action, details) values (?, ?, ?::jsonb);", baseSize * TablesScales.actionsLog) { i, ps ->
            ps.setObject(1, random.chooseOrNull(users))
            ps.setString(2, random.choose(actions))
            ps.setString(3, """{
                "comment": "${faker.lorem().sentence(5)}"
            }""".trimIndent())
        }
    }

    private fun generateDroids() {
        insert("insert into data.droids (id, sex, class, capacity, order_cost, maintenance_cost) values (?, ?::sex, ?, ?, ?, ?);", dorids) { i, id, ps ->
            ps.setObject(1, id)
            ps.setString(2, random.choose(sexEnum))
            ps.setString(3, faker.vehicle().fuelType())
            ps.setInt(4, random.nextInt(1, 100))
            ps.setDouble(5, random.nextDouble(100.0, 1000.0))
            ps.setDouble(6, random.nextDouble(50.0, 500.0))
        }
    }

    private fun generateProducts() {
        insert("insert into data.products (id, price, name) values (?, ?, ?);", products) { i, id, ps ->
            ps.setObject(1, id)
            ps.setDouble(2, random.nextDouble(1.0, 100.0))
            ps.setString(3, faker.food().dish())
        }
    }

    private fun generateAlcohol() {
        insert("insert into data.alcohol (product_id, brand, kind, volume, quality, strength) values (?, ?, ?, ?, ?, ?);", alcohols) { i, id, ps ->
            ps.setObject(1, id)
            ps.setString(2, faker.company().name())
            ps.setString(3, random.choose(alcoholsTypes))
            ps.setDouble(4, random.nextDouble(0.1, 5.0))
            ps.setDouble(5, random.nextDouble(0.0, 10.0))
            ps.setDouble(6, random.nextDouble(0.0, 95.0))
        }
    }

    private fun generateSnacks() {
        insert("insert into data.snacks (product_id, kind) values (?, ?);", snacks) { i, id, ps ->
            ps.setObject(1, id)
            ps.setString(2, faker.food().dish())
        }
    }

    private fun generateConversationTopics() {
        insert("insert into data.conversation_topics (id, name, description) values (?, ?, ?);", topics) { i, id, ps ->
            ps.setObject(1, id)
            ps.setString(2, topicsNames[i])
            ps.setString(3, "Interesting discussions about ${topicsNames[i]}")
        }
    }

    private fun generatePromotions() {
        insert("insert into data.promotions (id, price, name, promotion_time, available_time_hour, avalible_user) values (?, ?, ?, ?::tstzrange[], ?, ?);", promotions) { i, id, ps ->
            ps.setObject(1, id)
            ps.setDouble(2, random.nextDouble(10.0, 100.0))
            ps.setString(3, faker.name().nameWithMiddle())
            val tsranges = random.genTimeRanges(random.nextInt(0, 10)).map {(start, end) ->
                PGobject().apply {
                    type = "tstzrange"
                    value = "[${start.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)},${end.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)})"
                }
            }.toTypedArray()
            ps.setArray(4, connection.createArrayOf("tstzrange", tsranges))
//            ps.setObject(4, "{['2020-01-01 00:00:00+00', '2020-01-02 00:00:00+00')}")
            ps.setInt(5, random.nextInt(0, 24))
            ps.setObject(6, random.chooseOrNull(customers))
        }
    }

    private fun generateOrders() {
        insert("insert into data.orders (id, customer_id, promotion_id, status, type, address, ordered_for) values (?, ?, ?, ?::order_status, ?::order_type, ?, ?::tstzrange);", orders) { i, id, ps ->
            ps.setObject(1, id)
            ps.setObject(2, random.choose(customers))
            ps.setObject(3, random.chooseOrNull(promotions, 0.8f))
            ps.setString(4, random.choose(statusEnum))
            ps.setString(5, if (id.hashCode().absoluteValue % 100 > 0) typeEnum[0] else typeEnum[1])
            ps.setString(6, faker.address().fullAddress())
            ps.setObject(7, PGobject().apply {
                val year = random.nextInt(2025, 2030)
                val month = random.nextInt(1, 13)
                val day = random.nextInt(1, Month.of(month).maxLength())
                val from = random.nextInt(1, 20)
                val to = from + random.nextInt(1, 24 - from)
                type = "tstzrange"
                value = "['$year-$month-$day $from:00','$year-$month-$day $to:00']"
            })
        }
    }

    private fun generateOrderDroidsAndServiceLog() {
        connection.createStatement().use { ps ->
            orders.forEach { id ->
                val expectedDroids = random.nextInt(1,
                    if(id.hashCode().absoluteValue % 100 > 0) 2 else 600)
                val droids = random.chooseFrom(expectedDroids, dorids).toSet()

                droids.forEach { droid ->
                    ps.addBatch("""
                        insert into data.order_droids (order_id, droid_id, name) 
                        values ('$id'::uuid, '$droid'::uuid, '${faker.name().nameWithMiddle().replace("'", "")}') on conflict do nothing;
                    """.trimIndent())

                    val ts = random.nextInstant(
                        from = ZonedDateTime.of(2000, 1, 1, 1, 1, 1, 0, ZoneOffset.UTC).toInstant(),
                        to = ZonedDateTime.of(2025, 1, 1, 1, 1, 1, 0, ZoneOffset.UTC).toInstant()
                    ).toSqlTimestamp()

                    ps.addBatch("""
                        insert into data.service_log (technician_id, order_id, droid_id, serviced_at, reward) 
                        values ('${random.choose(technicians)}'::uuid, '$id'::uuid, '$droid'::uuid, '$ts', ${random.nextDouble(100.0, 10000.0)});
                    """.trimIndent())
                }
            }
            ps.executeBatch()
        }
    }


    private fun generateOrderProducts() {
        val ids = (1..baseSize * TablesScales.orderProducts).map {
            random.choose(orders) to random.choose(products)
        }.toSet()
        insert("insert into data.order_products (order_id, product_id, amount, sell_price, purchase_price, purchase_store) values (?, ?, ?, ?, ?, ?) on conflict do nothing;", ids.toList()) { i, id, ps ->
            ps.setObject(1, id.first)
            ps.setObject(2, id.second)
            ps.setInt(3, random.nextInt(1, 10))
            val price = random.nextDouble(100.0, 10000.0)
            ps.setDouble(4, price)
            ps.setDouble(5, price * random.nextDouble(0.5, 1.0))
            ps.setObject(6, random.choose(stores))

        }
    }

    private fun generateOrdersTopics() {
        insert("insert into data.orders_topics (order_id, topic_id) values (?, ?) on conflict do nothing;", baseSize * TablesScales.ordersTopics) { i, ps ->
            ps.setObject(1, random.choose(orders))
            ps.setObject(2, random.choose(topics))
        }
    }

    private fun generateSnackRecommendations() {
        insert("insert into data.snack_recommendations (snack_id, alcohol_id, topic_id, experience, score) values (?, ?, ?, ?::int4range, ?);", baseSize * TablesScales.snackRecommendations) { i, ps ->
            ps.setObject(1, random.choose(snacks))
            ps.setObject(2, random.chooseOrNull(alcohols))
            ps.setObject(3, random.chooseOrNull(topics))
            val low = random.nextInt(0, 100)
            val high = low + random.nextInt(1, 100)
            ps.setString(4, "[$low,$high)")
            ps.setInt(5, random.nextInt(0, 100))
        }
    }

    private fun generateStores() {
        insert("insert into data.stores (id, address, priority, schedule) values (?, ?, ?, ?::jsonb);", stores) { i, id, ps ->
            ps.setObject(1, id)
            ps.setString(2, faker.address().fullAddress())
            ps.setInt(3, random.nextInt(1, 10))
            ps.setString(4, "{\"schedule\":\"" + random.nextString() + "\"}")
        }
    }

    private fun generateStoreLots() {
        insert("insert into data.store_lots (product_id, store_id, price, in_stock) values (?, ?, ?, ?);", baseSize * TablesScales.storeLots) { i, ps ->
            ps.setObject(1, random.choose(products))
            ps.setObject(2, random.choose(stores))
            ps.setDouble(3, random.nextDouble(1.0, 100.0))
            ps.setInt(4, random.nextInt(0, 1000))
        }
    }

    private fun generatePromotionProducts() {
        insert("insert into data.promotion_products (promotion_id, product_id, amount) values (?, ?, ?) on conflict do nothing;", baseSize * TablesScales.promotionProducts) { i, ps ->
            ps.setObject(1, random.choose(promotions))
            ps.setObject(2, random.choose(products))
            ps.setInt(3, random.nextInt())
        }
    }

    private fun generatePromotionsTopics() {
        insert("insert into data.promotions_topics (promotion_id, topic_id) values (?, ?) on conflict do nothing;", baseSize * TablesScales.promotionsTopics) { i, ps ->
            ps.setObject(1, random.choose(promotions))
            ps.setObject(2, random.choose(topics))
        }
    }

}