package br.com.assemblenewtechnologies.ANTLogSync.ETL;

/**
 * Calculo de valor Medio Volume/Negocios
 */



import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;


public class B3VolumeNegociosLoad {

	private static final Long BULK_BATCH_INSERT_SIZE = 150000L;

	private static Logger LOGGER = LoggerFactory.getLogger(B3VolumeNegociosLoad.class);
	private static Connection connection;
	private static long start_time;
	private static long timer1;
	private static long timer2;
	private static long timer3;
	private static long timer4;
	private static long timer5;

	private static Map<String, Object> asset_book_values;
	private static Map<String, Map<String, Object>> buffer_last_values = new HashMap<String, Map<String, Object>>();

//	private final static String __dt_inicio = "2021-01-26 00:00:00";
//	private final static String __dt_fim = "2021-01-26 23:59:5";

//	private static Map<String, String> _days_to_process = new HashMap<String, String>();

	private static PreparedStatement preparedStatement;

	private static List<String> ativos_list = new ArrayList<String>();

	public static void main(String[] args) throws Exception {

		try {
			connection = DBConnectionHelper.getNewConn();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new Exception("No database connection...");
		}

		start_time = System.currentTimeMillis();
		LOGGER.info("Initializing B3 SignalLogger ETL phase 2 - 'Volume/Neogcios Analisys' ...");
		LOGGER.info("B3Log.B3SignalLogger SUM(VALUME, NEGOCIOS) GROUP BY RANGE TIME");
		LOGGER.info("Results will be at B3Log.B3ETL2VolumeNegocios table");

//		_days_to_process.put("2021-02-02 00:00:00", "2021-02-02 23:59:5");
//		_days_to_process.put("2021-01-29 00:00:00", "2021-01-29 23:59:5");
//		_days_to_process.put("2021-01-28 00:00:00", "2021-01-28 23:59:5");
//		_days_to_process.put("2021-01-27 00:00:00", "2021-01-27 23:59:5");
//		_days_to_process.put("2021-01-26 00:00:00", "2021-01-26 23:59:5");

		List<String> _day_to_process = new ArrayList<String>();
		_day_to_process.add("2021-01-26");
		_day_to_process.add("2021-01-27");
		_day_to_process.add("2021-01-28");
		_day_to_process.add("2021-01-29");
		_day_to_process.add("2021-02-02");

		LOGGER.info("Fetching assets from B3Log.B3SignalLogger from :" + _day_to_process.get(0) + " day");
		Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = stmt.executeQuery("select  ativo from B3Log.B3AtivosOpcoes group by ativo");
		while (rs.next()) {
			ativos_list.add(rs.getString("ativo"));
		}

		if (ativos_list.size() == 0) {
			throw new Exception("No mapped Assets found on B3Log.B3AtivosOpcoes");
		}

		for (String _dt_inicio : _day_to_process) {
//			String __dt_inicio = entry.getKey();
//			String __dt_fim = entry.getValue();

			try {
				for (String _ativo : ativos_list) {
					LOGGER.info("Fetching raw data from B3Log.B3SignalLogger:");
					LOGGER.info("asset:" + _ativo + " " + _dt_inicio);
					stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
					String sql = "select * from B3Log.B3SignalLoggerLevel1 a \n" + "where estado_atual= 'Aberto' \n"
							+ "AND a.asset like  substring('" + _ativo + "', '[A-Z]+')||'%' AND a.strike != 0 "
							+ "AND relogio >= TO_TIMESTAMP('" + _dt_inicio + " 00:00:00.0','YYYY-MM-DD HH24:MI:SS') \n"
							+ "AND a.relogio <= TO_TIMESTAMP('" + _dt_inicio
							+ " 23:59:59.9','YYYY-MM-DD HH24:MI:SS') \n" + "ORDER BY a.relogio ASC";

//					LOGGER.info(sql);
					rs = stmt.executeQuery(sql);
					int rows = 0;
					timer1 = System.currentTimeMillis();
					long db_load_time = timer1 - start_time;
					LOGGER.info("sql DB fetch execution time:" + db_load_time + " ms");

					Long asset_counter_total = 0L;
					Long asset_counter_changes = 0L;
					Long asset_counter_changes_total = 0L;
					Long asset_counter_batch_insert_total = 0L;
					boolean changes = false;
					String data;
					String hora;
					String asset;
					double valor_ativo;
					double ultimo;
					double strike;
					double oferta_compra;
					double oferta_venda;
					String vencimento;
					String validade;
					String estado_atual;
					Timestamp relogio;

					int VOC;
					int VOV;
					BigDecimal contratos_abertos;
					int negocios;
					int quantidade;
					Double volume;

					if (rs.last()) {
						rows = rs.getRow();
						rs.beforeFirst();
						LOGGER.info(
								"total records fetched on database for this period: " + rows + " for asset: " + _ativo);
						if (rows == 0)
							throw new Exception("No records found to analyse ETL Level2  between " + _dt_inicio
									+ " and end the end of day trades" + " on asset: " + _ativo);
					}

					String compiledQuery = "INSERT INTO B3Log.B3SignalLoggerLevel2Negocios("
							+ "data,hora,asset,valor_ativo,ultimo,strike,oferta_compra,oferta_venda,vencimento,validade,estado_atual,relogio_last_change,"
							+ "VOC, VOV, contratos_abertos, valor_medio_negocios, negocios, quantidade, volume) VALUES"
							+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
					preparedStatement = connection.prepareStatement(compiledQuery);

					while (rs.next()) {
						data = rs.getString("data");
						hora = rs.getString("hora");
						asset = rs.getString("asset");
						valor_ativo = rs.getDouble("valor_ativo");
						ultimo = rs.getDouble("ultimo");
						strike = rs.getDouble("strike");
						oferta_compra = rs.getDouble("oferta_compra");
						oferta_venda = rs.getDouble("oferta_venda");
						vencimento = rs.getString("vencimento");
						validade = rs.getString("validade");
						estado_atual = rs.getString("estado_atual");
						relogio = rs.getTimestamp("relogio");

						VOC = rs.getInt("VOC");
						VOV = rs.getInt("VOV");
						contratos_abertos = rs.getBigDecimal("contratos_abertos");
						negocios = rs.getInt("negocios");
						quantidade = rs.getInt("quantidade");
						volume = rs.getDouble("volume");

						asset_book_values = new HashMap<String, Object>();
						asset_book_values.put("data", data);
						asset_book_values.put("hora", hora);
						asset_book_values.put("asset", asset);
						asset_book_values.put("valor_ativo", valor_ativo);
						asset_book_values.put("ultimo", ultimo);
						asset_book_values.put("strike", strike);
						asset_book_values.put("oferta_compra", oferta_compra);
						asset_book_values.put("oferta_venda", oferta_venda);
						asset_book_values.put("vencimento", vencimento);
						asset_book_values.put("validade", validade);
						asset_book_values.put("estado_atual", estado_atual);
						asset_book_values.put("relogio", relogio.toString());

						asset_book_values.put("VOC", VOC);
						asset_book_values.put("VOV", VOV);
						asset_book_values.put("contratos_abertos", contratos_abertos);
						asset_book_values.put("negocios", negocios);
						asset_book_values.put("quantidade", quantidade);
						asset_book_values.put("volume", volume);

						if (!buffer_last_values.containsKey(asset)) {
							LOGGER.info("Adding asset:" + asset + " to buffer");
							asset_counter_total++;
							buffer_last_values.put(asset, new HashMap<String, Object>(asset_book_values));

							asset_counter_changes++;
							asset_counter_changes_total++;

							continue;
						}

//						double buffer_valor_ativo = (double) buffer_last_values.get(asset).get("valor_ativo");
//						double buffer_ultimo = (double) buffer_last_values.get(asset).get("ultimo");
//						double buffer_strike = (double) buffer_last_values.get(asset).get("strike");
//						double buffer_oferta_compra = (double) buffer_last_values.get(asset).get("oferta_compra");
//						double buffer_oferta_venda = (double) buffer_last_values.get(asset).get("oferta_venda");

//						int buffer_VOC = (int) buffer_last_values.get(asset).get("VOC");
//						int buffer_VOV = (int) buffer_last_values.get(asset).get("VOV");
//						BigDecimal buffer_contratos_abertos = (BigDecimal) buffer_last_values.get(asset)
//								.get("contratos_abertos");
						int buffer_negocios = (int) buffer_last_values.get(asset).get("negocios");
						int buffer_quantidade = (int) buffer_last_values.get(asset).get("quantidade");
						Double buffer_volume = (Double) buffer_last_values.get(asset).get("volume");
//						String buffer_estado_atual = (String) buffer_last_values.get(asset).get("estado_atual");

//						if (buffer_valor_ativo != valor_ativo)
//							changes = true;
//						if (buffer_preco_opcao != preco_opcao)
//							changes = true;
//						if (buffer_strike != strike)
//							changes = true;
//						if (buffer_oferta_compra != oferta_compra)
//							changes = true;
//						if (buffer_oferta_venda != oferta_venda)
//							changes = true;
						//
//						if (buffer_VOC != VOC)
//							changes = true;
//						if (buffer_VOV != VOV)
//							changes = true;
//						if (!buffer_contratos_abertos.equals(contratos_abertos))
//							changes = true;
						if (buffer_negocios != negocios)
							changes = true;
//						if (buffer_quantidade != quantidade)
//							changes = true;
//						if (!buffer_volume.equals(volume))
//							changes = true;
						//
//						if (!buffer_estado_atual.equals(estado_atual))
//							changes = true;

						// EH DIFERENTE DO ANTERIOR DO MESMO ASSET
						if (changes) {
							buffer_last_values.put(asset, new HashMap<String, Object>(asset_book_values));
							asset_counter_changes++;
							asset_counter_changes_total++;
							changes = false;

							java.sql.Date _date = java.sql.Date.valueOf(data);
							java.sql.Date _vencimento = java.sql.Date.valueOf(vencimento);
							java.sql.Date _validade = java.sql.Date.valueOf(validade);

							SimpleDateFormat formatter = new SimpleDateFormat("hh:mm:ss");
							Time _hora = new Time(formatter.parse(hora).getTime());

							double valor_medio_negocios = volume / quantidade;

							preparedStatement.setDate(1, _date);
							preparedStatement.setTime(2, _hora);
							preparedStatement.setString(3, asset);
							preparedStatement.setDouble(4, valor_ativo);
							preparedStatement.setDouble(5, ultimo);
							preparedStatement.setDouble(6, strike);
							preparedStatement.setDouble(7, oferta_compra);
							preparedStatement.setDouble(8, oferta_venda);
							preparedStatement.setDate(9, _vencimento);
							preparedStatement.setDate(10, _validade);
							preparedStatement.setString(11, estado_atual);
							preparedStatement.setTimestamp(12, relogio);
							preparedStatement.setInt(13, VOC);
							preparedStatement.setInt(14, VOV);
							preparedStatement.setBigDecimal(15, contratos_abertos);
//							preparedStatement.setInt(16, (negocios - buffer_negocios));
//							preparedStatement.setInt(17, (quantidade - buffer_quantidade));
//							preparedStatement.setDouble(18, (volume - buffer_volume));
							preparedStatement.setDouble(16, valor_medio_negocios);
							preparedStatement.setInt(17, (negocios));
							preparedStatement.setInt(18, (quantidade));
							preparedStatement.setDouble(19, (volume));

							preparedStatement.addBatch();
							preparedStatement.clearParameters();
						}

						/*
						 * BULK INSERT OR IF TOTAL ROWS FETCHED < BULK_SIZE
						 */
						if (asset_counter_changes > BULK_BATCH_INSERT_SIZE || rows == (asset_counter_total + 1)) {
							timer2 = System.currentTimeMillis();

							/*
							 * execute BATCH INSERT OF BULK_SIZE preperadStatement
							 */
							int[] inserted = preparedStatement.executeBatch();

							if (inserted != null && inserted[0] < 0)
								throw new Exception("error on bulk insert at record: " + asset_counter_total
										+ " from tables b3signallogger \n" + "records before " + asset_counter_total
										+ " counter, may be already inserted on database B3SignalLoggerLevel1");

							timer3 = System.currentTimeMillis();
							long diff_time = timer3 - timer2;
							LOGGER.info("BULK insert of: " + inserted.length + " inserted.lenght");
							LOGGER.info("Total time to INSERT: " + inserted.length + " registros " + diff_time + " ms");
//							connection.commit();
							preparedStatement.clearBatch();

							asset_counter_changes = 0L;
							asset_counter_batch_insert_total++;
						}
						asset_counter_total++;
					}

					timer4 = System.currentTimeMillis();
					long diff_time = timer4 - timer1;

					LOGGER.info("Total records processed: " + asset_counter_total);
					LOGGER.info("Total records changes: " + asset_counter_changes);
					LOGGER.info("Total BULK batch inserts: " + asset_counter_batch_insert_total + " of "
							+ BULK_BATCH_INSERT_SIZE + " records");
					LOGGER.info("Total time to process: " + diff_time + "ms asset: " + _ativo);

					asset_counter_total = 0L;
					asset_counter_changes = 0L;
					asset_counter_changes_total = 0L;
					asset_counter_batch_insert_total = 0L;
					buffer_last_values = new HashMap<String, Map<String, Object>>();
				}

				timer5 = System.currentTimeMillis();
				long _diff_time = timer5 - start_time;
				LOGGER.info("Total time to process " + ativos_list.size() + " assets: " + _diff_time + "ms ");

			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				preparedStatement.close();
				connection.close();

				return;
			}

			preparedStatement.close();
		}

		connection.close();
	}
}