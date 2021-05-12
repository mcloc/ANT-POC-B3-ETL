package br.com.assemblenewtechnologies.ANTLogSync.model.B3;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

public class B3Sample {
	private String asset;
	private Date data;
	private Time hora;
	private double ultimo;
	private double Strike;
	private int negocios;
	private int quantidade;
	private double volume;
	private double oferta_compra;
	private double oferta_venda;
	private int VOC;
	private int VOV;
	private Date vencimento;
	private Date validade;
	private int contratos_abertos;
	private String estado_atual;
	private Timestamp relogio;


	public void hidrateFromResultSet(ResultSet rs) throws SQLException {
		try {
			asset = rs.getString("asset");
			data = rs.getDate("data");
			hora = rs.getTime("hora");
			ultimo = rs.getDouble("ultimo");
			Strike = rs.getDouble("Strike");
			negocios = rs.getInt("negocios");
			quantidade = rs.getInt("quantidade");
			volume = rs.getDouble("volume");
			oferta_compra = rs.getDouble("oferta_compra");
			oferta_venda = rs.getDouble("oferta_venda");
			VOC = rs.getInt("VOC");
			VOV = rs.getInt("VOV");
			vencimento = rs.getDate("vencimento");
			validade = rs.getDate("validade");
			contratos_abertos = rs.getInt("contratos_abertos");
			estado_atual = rs.getString("estado_atual");
			relogio = rs.getTimestamp("relogio");

		} catch (SQLException e) {
			throw new SQLException(e.getMessage());
		}
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
	public Date getData() {
		return data;
	}


	/**
	 * @param data the data to set
	 */
	public void setData(Date data) {
		this.data = data;
	}


	/**
	 * @return the hora
	 */
	public Time getHora() {
		return hora;
	}


	/**
	 * @param hora the hora to set
	 */
	public void setHora(Time hora) {
		this.hora = hora;
	}


	/**
	 * @return the ultimo
	 */
	public double getUltimo() {
		return ultimo;
	}


	/**
	 * @param ultimo the ultimo to set
	 */
	public void setUltimo(double ultimo) {
		this.ultimo = ultimo;
	}


	/**
	 * @return the strike
	 */
	public double getStrike() {
		return Strike;
	}


	/**
	 * @param strike the strike to set
	 */
	public void setStrike(double strike) {
		Strike = strike;
	}


	/**
	 * @return the negocios
	 */
	public int getNegocios() {
		return negocios;
	}


	/**
	 * @param negocios the negocios to set
	 */
	public void setNegocios(int negocios) {
		this.negocios = negocios;
	}


	/**
	 * @return the quantidade
	 */
	public int getQuantidade() {
		return quantidade;
	}


	/**
	 * @param quantidade the quantidade to set
	 */
	public void setQuantidade(int quantidade) {
		this.quantidade = quantidade;
	}


	/**
	 * @return the volume
	 */
	public double getVolume() {
		return volume;
	}


	/**
	 * @param volume the volume to set
	 */
	public void setVolume(double volume) {
		this.volume = volume;
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
	 * @return the vOC
	 */
	public int getVOC() {
		return VOC;
	}


	/**
	 * @param vOC the vOC to set
	 */
	public void setVOC(int vOC) {
		VOC = vOC;
	}


	/**
	 * @return the vOV
	 */
	public int getVOV() {
		return VOV;
	}


	/**
	 * @param vOV the vOV to set
	 */
	public void setVOV(int vOV) {
		VOV = vOV;
	}


	/**
	 * @return the vencimento
	 */
	public Date getVencimento() {
		return vencimento;
	}


	/**
	 * @param vencimento the vencimento to set
	 */
	public void setVencimento(Date vencimento) {
		this.vencimento = vencimento;
	}


	/**
	 * @return the validade
	 */
	public Date getValidade() {
		return validade;
	}


	/**
	 * @param validade the validade to set
	 */
	public void setValidade(Date validade) {
		this.validade = validade;
	}


	/**
	 * @return the contratos_abertos
	 */
	public int getContratos_abertos() {
		return contratos_abertos;
	}


	/**
	 * @param contratos_abertos the contratos_abertos to set
	 */
	public void setContratos_abertos(int contratos_abertos) {
		this.contratos_abertos = contratos_abertos;
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
	public Timestamp getRelogio() {
		return relogio;
	}


	/**
	 * @param relogio the relogio to set
	 */
	public void setRelogio(Timestamp relogio) {
		this.relogio = relogio;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(Strike);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + VOC;
		result = prime * result + VOV;
		result = prime * result + ((asset == null) ? 0 : asset.hashCode());
		result = prime * result + contratos_abertos;
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		result = prime * result + ((estado_atual == null) ? 0 : estado_atual.hashCode());
		result = prime * result + ((hora == null) ? 0 : hora.hashCode());
		result = prime * result + negocios;
		temp = Double.doubleToLongBits(oferta_compra);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(oferta_venda);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + quantidade;
		result = prime * result + ((relogio == null) ? 0 : relogio.hashCode());
		temp = Double.doubleToLongBits(ultimo);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((validade == null) ? 0 : validade.hashCode());
		result = prime * result + ((vencimento == null) ? 0 : vencimento.hashCode());
		temp = Double.doubleToLongBits(volume);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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
		B3Sample other = (B3Sample) obj;
		if (Double.doubleToLongBits(Strike) != Double.doubleToLongBits(other.Strike))
			return false;
		if (VOC != other.VOC)
			return false;
		if (VOV != other.VOV)
			return false;
		if (asset == null) {
			if (other.asset != null)
				return false;
		} else if (!asset.equals(other.asset))
			return false;
		if (contratos_abertos != other.contratos_abertos)
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
		if (negocios != other.negocios)
			return false;
		if (Double.doubleToLongBits(oferta_compra) != Double.doubleToLongBits(other.oferta_compra))
			return false;
		if (Double.doubleToLongBits(oferta_venda) != Double.doubleToLongBits(other.oferta_venda))
			return false;
		if (quantidade != other.quantidade)
			return false;
		if (relogio == null) {
			if (other.relogio != null)
				return false;
		} else if (!relogio.equals(other.relogio))
			return false;
		if (Double.doubleToLongBits(ultimo) != Double.doubleToLongBits(other.ultimo))
			return false;
		if (validade == null) {
			if (other.validade != null)
				return false;
		} else if (!validade.equals(other.validade))
			return false;
		if (vencimento == null) {
			if (other.vencimento != null)
				return false;
		} else if (!vencimento.equals(other.vencimento))
			return false;
		if (Double.doubleToLongBits(volume) != Double.doubleToLongBits(other.volume))
			return false;
		return true;
	}


	

}
