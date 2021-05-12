package br.com.assemblenewtechnologies.ANTLogSync.model.B3;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class B3SampleTransoformLevel1 {
	private String asset;
	private String data;
	private String hora;
	private double valor_ativo;
	private double preco_opcao;
	private double strike;
	private double oferta_compra;
	private double oferta_venda;
	private String vencimento;
	private String validade;
	private String estado_atual;
	private String relogio;


	public void hidrateFromResultSet(ResultSet rs) throws SQLException {
		try {
			asset = rs.getString("asset");
			data = rs.getString("data");
			hora = rs.getString("hora");
			valor_ativo = rs.getDouble("valor_ativo");
			preco_opcao = rs.getDouble("preco_opcao");
			strike = rs.getDouble("strike");
			oferta_compra = rs.getDouble("oferta_compra");
			oferta_venda = rs.getDouble("oferta_venda");
			vencimento = rs.getString("vencimento");
			validade = rs.getString("validade");
			estado_atual = rs.getString("estado_atual");
			relogio = rs.getString("relogio");

		} catch (SQLException e) {
			throw new SQLException(e.getMessage());
		}
	}
	

	public void hidrateFromMapValues(Map<String, Object> asset_book_values) {
		asset = (String) asset_book_values.get("asset");
		data = (String) asset_book_values.get("data");
		hora = (String) asset_book_values.get("hora");
		valor_ativo = (double) asset_book_values.get("valor_ativo");
		preco_opcao = (double) asset_book_values.get("preco_opcao");
		
		if(asset_book_values.get("strike") == null)
			strike = 0.00;
		else
			strike = (double) asset_book_values.get("strike");
		
		if(asset_book_values.get("oferta_compra") == null)
			oferta_compra = 0.00;
		else
			oferta_compra = (double) asset_book_values.get("oferta_compra");
		
		if(asset_book_values.get("oferta_venda") == null)
			oferta_venda = 0.00;
		else
			oferta_venda = (double) asset_book_values.get("oferta_venda");		
		
		vencimento = (String) asset_book_values.get("vencimento");
		validade = (String) asset_book_values.get("validade");
		estado_atual = (String) asset_book_values.get("estado_atual");
		relogio = (String) asset_book_values.get("relogio");
		
	}



	/**
	 * @return the asset
	 */
	public String getAsset() {
		return asset;
	}


	/**
	 * @param asset the asset to set
	 */
	public void setAsset(String asset) {
		this.asset = asset;
	}


	/**
	 * @return the data
	 */
	public String getData() {
		return data;
	}


	/**
	 * @param data the data to set
	 */
	public void setData(String data) {
		this.data = data;
	}


	/**
	 * @return the hora
	 */
	public String getHora() {
		return hora;
	}


	/**
	 * @param hora the hora to set
	 */
	public void setHora(String hora) {
		this.hora = hora;
	}

	/**
	 * @return the valor_ativo
	 */
	public double getValor_ativo() {
		return valor_ativo;
	}


	/**
	 * @param valor_ativo the valor_ativo to set
	 */
	public void setValor_ativo(double valor_ativo) {
		this.valor_ativo = valor_ativo;
	}


	/**
	 * @return the preco_opcao
	 */
	public double getPreco_opcao() {
		return preco_opcao;
	}


	/**
	 * @param preco_opcao the preco_opcao to set
	 */
	public void setPreco_opcao(double preco_opcao) {
		this.preco_opcao = preco_opcao;
	}


	/**
	 * @return the strike
	 */
	public double getStrike() {
		return strike;
	}


	/**
	 * @param strike the strike to set
	 */
	public void setStrike(double strike) {
		strike = strike;
	}


	/**
	 * @return the oferta_compra
	 */
	public double getOferta_compra() {
		return oferta_compra;
	}


	/**
	 * @param oferta_compra the oferta_compra to set
	 */
	public void setOferta_compra(double oferta_compra) {
		this.oferta_compra = oferta_compra;
	}


	/**
	 * @return the oferta_venda
	 */
	public double getOferta_venda() {
		return oferta_venda;
	}


	/**
	 * @param oferta_venda the oferta_venda to set
	 */
	public void setOferta_venda(double oferta_venda) {
		this.oferta_venda = oferta_venda;
	}


	/**
	 * @return the vencimento
	 */
	public String getVencimento() {
		return vencimento;
	}


	/**
	 * @param vencimento the vencimento to set
	 */
	public void setVencimento(String vencimento) {
		this.vencimento = vencimento;
	}


	/**
	 * @return the validade
	 */
	public String getValidade() {
		return validade;
	}


	/**
	 * @param validade the validade to set
	 */
	public void setValidade(String validade) {
		this.validade = validade;
	}


	/**
	 * @return the estado_atual
	 */
	public String getEstado_atual() {
		return estado_atual;
	}


	/**
	 * @param estado_atual the estado_atual to set
	 */
	public void setEstado_atual(String estado_atual) {
		this.estado_atual = estado_atual;
	}


	/**
	 * @return the relogio
	 */
	public String getRelogio() {
		return relogio;
	}


	/**
	 * @param relogio the relogio to set
	 */
	public void setRelogio(String relogio) {
		this.relogio = relogio;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(strike);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((asset == null) ? 0 : asset.hashCode());
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		result = prime * result + ((estado_atual == null) ? 0 : estado_atual.hashCode());
		result = prime * result + ((hora == null) ? 0 : hora.hashCode());
		temp = Double.doubleToLongBits(oferta_compra);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(oferta_venda);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(preco_opcao);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((relogio == null) ? 0 : relogio.hashCode());
		result = prime * result + ((validade == null) ? 0 : validade.hashCode());
		temp = Double.doubleToLongBits(valor_ativo);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((vencimento == null) ? 0 : vencimento.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		B3SampleTransoformLevel1 other = (B3SampleTransoformLevel1) obj;
		if (Double.doubleToLongBits(strike) != Double.doubleToLongBits(other.strike))
			return false;
		if (asset == null) {
			if (other.asset != null)
				return false;
		} else if (!asset.equals(other.asset))
			return false;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		if (estado_atual == null) {
			if (other.estado_atual != null)
				return false;
		} else if (!estado_atual.equals(other.estado_atual))
			return false;
		if (hora == null) {
			if (other.hora != null)
				return false;
		} else if (!hora.equals(other.hora))
			return false;
		if (Double.doubleToLongBits(oferta_compra) != Double.doubleToLongBits(other.oferta_compra))
			return false;
		if (Double.doubleToLongBits(oferta_venda) != Double.doubleToLongBits(other.oferta_venda))
			return false;
		if (Double.doubleToLongBits(preco_opcao) != Double.doubleToLongBits(other.preco_opcao))
			return false;
		if (relogio == null) {
			if (other.relogio != null)
				return false;
		} else if (!relogio.equals(other.relogio))
			return false;
		if (validade == null) {
			if (other.validade != null)
				return false;
		} else if (!validade.equals(other.validade))
			return false;
		if (Double.doubleToLongBits(valor_ativo) != Double.doubleToLongBits(other.valor_ativo))
			return false;
		if (vencimento == null) {
			if (other.vencimento != null)
				return false;
		} else if (!vencimento.equals(other.vencimento))
			return false;
		return true;
	}


	


}
