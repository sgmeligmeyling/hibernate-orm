/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.orm.test.query.thingy;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MapsId;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Tuple;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.JoinType;
import org.hibernate.annotations.SortNatural;
import org.hibernate.query.criteria.HibernateCriteriaBuilder;
import org.hibernate.query.criteria.JpaCriteriaQuery;
import org.hibernate.query.criteria.JpaDerivedJoin;
import org.hibernate.query.criteria.JpaRoot;
import org.hibernate.query.criteria.JpaSubQuery;
import org.hibernate.query.sqm.tree.SqmJoinType;
import org.hibernate.testing.orm.junit.DialectFeatureChecks;
import org.hibernate.testing.orm.junit.DomainModel;
import org.hibernate.testing.orm.junit.RequiresDialectFeature;
import org.hibernate.testing.orm.junit.SessionFactory;
import org.hibernate.testing.orm.junit.SessionFactoryScope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DomainModel(annotatedClasses = { LateralJoinTests3.Inningszaak.class, LateralJoinTests3.Betaling.class, LateralJoinTests3.BetalingsRegeling.class, LateralJoinTests3.Betaalplan.class, LateralJoinTests3.Totaalvordering.class, LateralJoinTests3.InningzaakStatus.class, LateralJoinTests3.Status.class })
@SessionFactory
@RequiresDialectFeature(feature = DialectFeatureChecks.SupportsSubqueryInOnClause.class)
@RequiresDialectFeature(feature = DialectFeatureChecks.SupportsOrderByInCorrelatedSubquery.class)
public class LateralJoinTests3 {

	@Test
	public void test(SessionFactoryScope scope) {
		scope.inTransaction(
			session -> {
				HibernateCriteriaBuilder criteriaBuilder = session.getCriteriaBuilder();
				JpaCriteriaQuery<Tuple> query = criteriaBuilder.createQuery(Tuple.class);

				JpaRoot<Inningszaak> inningszaak = query.from(Inningszaak.class);
				Join<Inningszaak, Totaalvordering> totaalvordering = inningszaak.join("totaalvordering");

				Join<Inningszaak, InningzaakStatus> zaakStatus = inningszaak.join("statusHistorie");
				zaakStatus.on(criteriaBuilder.isTrue(zaakStatus.get("actief")));

				Join<InningzaakStatus, Status> status = zaakStatus.join("status");

				JpaSubQuery<Tuple> betalingJpaSubQuery = query.subquery(Tuple.class);
				Join<Inningszaak, Betaling> betaling = betalingJpaSubQuery.correlate(inningszaak).join("betalingen");
				betalingJpaSubQuery.orderBy(criteriaBuilder.desc(betaling.get("datum")));
				betalingJpaSubQuery.multiselect(
						betaling.get("bedrag").alias("bedragLaatsteBetaling"),
						betaling.get("datum").alias("datumLaatsteBetaling")
				);
				betalingJpaSubQuery.fetch(1);

				JpaDerivedJoin<Tuple> laatsteBetaling = inningszaak.joinLateral(betalingJpaSubQuery, SqmJoinType.LEFT);
				laatsteBetaling.alias("laatsteBetaling");

				Join<Inningszaak, BetalingsRegeling> betalingsRegelingJoin = inningszaak.join("betalingsRegeling", JoinType.LEFT);

				JpaSubQuery<Tuple> betaalPlanSubquery = query.subquery(Tuple.class);
				Join<BetalingsRegeling, Betaalplan> betaalplanSetJoin = betaalPlanSubquery.correlate(betalingsRegelingJoin).join("betaalplannen");
				betaalPlanSubquery.orderBy(criteriaBuilder.asc(betaalplanSetJoin.get("ingangsdatum")));
				betaalPlanSubquery.fetch(1);
				betaalPlanSubquery.multiselect(
						betaalplanSetJoin.get("bedrag").alias("termijnbedrag"),
						betaalplanSetJoin.get("ingangsdatum").alias("ingangsdatumBetaaltermijn")
				);

				JpaDerivedJoin<Tuple> betaalplan = inningszaak.joinLateral(betaalPlanSubquery, SqmJoinType.LEFT);
				betaalplan.alias("betaalplan");

				query.multiselect(
//						inningszaak.get("cjibNummer"),
//						inningszaak.get("beschikkingsnummer"),
//						inningszaak.get("aanmaakDatum"),
						status.get("naam"),
						zaakStatus.get("reden"),
						totaalvordering.get("bedrag"),
						totaalvordering.get("bedragReedsBetaald"),
						totaalvordering.get("openstaandBedrag"),
						laatsteBetaling.get("bedragLaatsteBetaling"),
						laatsteBetaling.get("datumLaatsteBetaling"),
						betaalplan.get("termijnbedrag"),
						betaalplan.get("ingangsdatumBetaaltermijn"),
						criteriaBuilder.or(
								criteriaBuilder.equal(totaalvordering.get("openstaandBedrag"), criteriaBuilder.literal(BigDecimal.ZERO)),
								criteriaBuilder.greaterThanOrEqualTo(laatsteBetaling.get("bedragLaatsteBetaling"), betaalplan.get("termijnbedrag"))
						)
				);

				List<Tuple> resultList = session.createQuery(query).getResultList();

				assertEquals(1, 1);
			}
		);
	}

	@BeforeEach
	public void prepareTestData(SessionFactoryScope scope) {
		scope.inTransaction( (session) -> {
			final Inningszaak inningszaak = new Inningszaak(1);
			final Inningszaak inningszaak2 = new Inningszaak(2);

			final Totaalvordering totaalvordering = new Totaalvordering(1, inningszaak, new BigDecimal(0), new BigDecimal(0), new BigDecimal(0));
			final Totaalvordering totaalvordering2 = new Totaalvordering(2, inningszaak2, new BigDecimal(0), new BigDecimal(0), new BigDecimal(0));

			final Status status = new Status(1, "Status");
			final Status status2 = new Status(2, "Status 2");

			final InningzaakStatus inningzaakStatus = new InningzaakStatus(1, true, "Reden", inningszaak, status);
			final InningzaakStatus inningzaakStatus2 = new InningzaakStatus(2, false, "Reden 2", inningszaak, status2);
			final InningzaakStatus inningzaakStatus3 = new InningzaakStatus(3, false, "Reden 3", inningszaak2, status);
			final InningzaakStatus inningzaakStatus4 = new InningzaakStatus(4, true, "Reden 4", inningszaak2, status2);

			final Betaling betaling = new Betaling(1, LocalDate.now(), new BigDecimal(5), inningszaak);
			final Betaling betaling2 = new Betaling(2, LocalDate.now().minus(1, ChronoUnit.DAYS), new BigDecimal(10), inningszaak);
			final Betaling betaling3 = new Betaling(3, LocalDate.now(), new BigDecimal(20), inningszaak2);
			final Betaling betaling4 = new Betaling(4, LocalDate.now().minus(1, ChronoUnit.DAYS), new BigDecimal(3), inningszaak2);

			final Betaalplan betaalplan = new Betaalplan(123, LocalDate.now(), new BigDecimal(5));
			final Betaalplan betaalplan2 = new Betaalplan(456, LocalDate.now().plus(1, ChronoUnit.MONTHS), new BigDecimal(10));
			final Betaalplan betaalplan3 = new Betaalplan(789, LocalDate.now(), new BigDecimal(4));
			final Betaalplan betaalplan4 = new Betaalplan(156, LocalDate.now().plus(2, ChronoUnit.MONTHS), new BigDecimal(10));

			final BetalingsRegeling betalingsRegeling = new BetalingsRegeling(1, inningszaak, new TreeSet<>(Arrays.asList(betaalplan, betaalplan2)));
			final BetalingsRegeling betalingsRegeling2 = new BetalingsRegeling(2, inningszaak2, new TreeSet<>(Arrays.asList(betaalplan3, betaalplan4)));

			session.persist(inningszaak);
			session.persist(inningszaak2);
			session.persist(totaalvordering);
			session.persist(totaalvordering2);
			session.persist(status);
			session.persist(status2);
			session.persist(inningzaakStatus);
			session.persist(inningzaakStatus2);
			session.persist(inningzaakStatus3);
			session.persist(inningzaakStatus4);
			session.persist(betaling);
			session.persist(betaling2);
			session.persist(betaling3);
			session.persist(betaling4);
			session.persist(betalingsRegeling);
			session.persist(betalingsRegeling2);
		} );
	}

	private <T> void verifySame(T criteriaResult, T hqlResult, Consumer<T> verifier) {
		verifier.accept( criteriaResult );
		verifier.accept( hqlResult );
	}

	@AfterEach
	public void dropTestData(SessionFactoryScope scope) {
		scope.inTransaction( (session) -> {
		} );
	}

	@Entity
	@Table( name = "inningszaak" )
	public static class Inningszaak {

		@Id
		private Integer id;

		@OneToOne(mappedBy = "inningszaak", optional = false)
		private Totaalvordering totaalvordering;

		@OneToOne(mappedBy = "inningszaak", fetch = FetchType.LAZY)
		private BetalingsRegeling betalingsRegeling;

		@OneToMany(mappedBy = "inningszaak")
		private List<Betaling> betalingen = new ArrayList<>();

		@OneToMany(mappedBy = "inningszaak")
		private List<InningzaakStatus> statusHistorie = new ArrayList<>();

		public Inningszaak() {
		}

		public Inningszaak(Integer id) {
			this.id = id;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public Totaalvordering getTotaalvordering() {
			return totaalvordering;
		}

		public void setTotaalvordering(Totaalvordering totaalvordering) {
			this.totaalvordering = totaalvordering;
		}

		public BetalingsRegeling getBetalingsRegeling() {
			return betalingsRegeling;
		}

		public void setBetalingsRegeling(BetalingsRegeling betalingsRegeling) {
			this.betalingsRegeling = betalingsRegeling;
		}

		public List<Betaling> getBetalingen() {
			return betalingen;
		}

		public void setBetalingen(List<Betaling> betalingen) {
			this.betalingen = betalingen;
		}

		public List<InningzaakStatus> getStatusHistorie() {
			return statusHistorie;
		}

		public void setStatusHistorie(List<InningzaakStatus> statusHistorie) {
			this.statusHistorie = statusHistorie;
		}
	}

	@Entity
	@Table( name = "betaling" )
	public static class Betaling {

		@Id
		private Integer id;

		private LocalDate datum;

		private BigDecimal bedrag;

		@ManyToOne(optional = false)
		private Inningszaak inningszaak;

		public Betaling() {
		}

		public Betaling(Integer id, LocalDate datum, BigDecimal bedrag, Inningszaak inningszaak) {
			this.id = id;
			this.datum = datum;
			this.bedrag = bedrag;
			this.inningszaak = inningszaak;
		}

		public Integer getId() {
			return id;
		}

		public LocalDate getDatum() {
			return datum;
		}

		public BigDecimal getBedrag() {
			return bedrag;
		}

		public Inningszaak getInningszaak() {
			return inningszaak;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public void setDatum(LocalDate datum) {
			this.datum = datum;
		}

		public void setBedrag(BigDecimal bedrag) {
			this.bedrag = bedrag;
		}

		public void setInningszaak(Inningszaak inningszaak) {
			this.inningszaak = inningszaak;
		}
	}

	@Entity
	@Table( name = "inningzaakStatus" )
	public static class InningzaakStatus {

		@Id
		private Integer id;

		private Boolean actief;

		private String reden;

		@ManyToOne(optional = false)
		private Inningszaak inningszaak;

		@ManyToOne(optional = false)
		private Status status;

		public InningzaakStatus() {
		}

		public InningzaakStatus(Integer id, Boolean actief, String reden, Inningszaak inningszaak, Status status) {
			this.id = id;
			this.actief = actief;
			this.reden = reden;
			this.inningszaak = inningszaak;
			this.status = status;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public Boolean getActief() {
			return actief;
		}

		public void setActief(Boolean actief) {
			this.actief = actief;
		}

		public String getReden() {
			return reden;
		}

		public void setReden(String reden) {
			this.reden = reden;
		}

		public Inningszaak getInningszaak() {
			return inningszaak;
		}

		public void setInningszaak(Inningszaak inningszaak) {
			this.inningszaak = inningszaak;
		}

		public Status getStatus() {
			return status;
		}

		public void setStatus(Status status) {
			this.status = status;
		}
	}

	@Entity
	@Table( name = "status" )
	public static class Status {

		@Id
		private Integer id;

		private String naam;

		public Status() {
		}

		public Status(Integer id, String naam) {
			this.id = id;
			this.naam = naam;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getNaam() {
			return naam;
		}

		public void setNaam(String naam) {
			this.naam = naam;
		}
	}

	@Entity
	@Table( name = "betalingsRegeling" )
	public static class BetalingsRegeling {

		@Id
		private Integer id;

		@MapsId
		@OneToOne(fetch = FetchType.LAZY)
		private Inningszaak inningszaak;

		@SortNatural
		@ElementCollection
		private SortedSet<Betaalplan> betaalplannen;

		public BetalingsRegeling() {
		}

		public BetalingsRegeling(Integer id, Inningszaak inningszaak, SortedSet<Betaalplan> betaalplannen) {
			this.id = id;
			this.inningszaak = inningszaak;
			this.betaalplannen = betaalplannen;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public Inningszaak getInningszaak() {
			return inningszaak;
		}

		public void setInningszaak(Inningszaak inningszaak) {
			this.inningszaak = inningszaak;
		}

		public SortedSet<Betaalplan> getBetaalplannen() {
			return betaalplannen;
		}

		public void setBetaalplannen(SortedSet<Betaalplan> betaalplannen) {
			this.betaalplannen = betaalplannen;
		}
	}

	@Entity
	@Table( name = "totaalvordering" )
	public static class Totaalvordering {

		@Id
		private Integer id;

		@MapsId
		@OneToOne(fetch = FetchType.LAZY)
		private Inningszaak inningszaak;

		private BigDecimal bedrag;

		private BigDecimal bedragReedsBetaald;

		private BigDecimal openstaandBedrag;

		public Totaalvordering() {
		}

		public Totaalvordering(Integer id, Inningszaak inningszaak, BigDecimal bedrag, BigDecimal bedragReedsBetaald, BigDecimal openstaandBedrag) {
			this.id = id;
			this.inningszaak = inningszaak;
			this.bedrag = bedrag;
			this.bedragReedsBetaald = bedragReedsBetaald;
			this.openstaandBedrag = openstaandBedrag;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public Inningszaak getInningszaak() {
			return inningszaak;
		}

		public void setInningszaak(Inningszaak inningszaak) {
			this.inningszaak = inningszaak;
		}

		public BigDecimal getBedrag() {
			return bedrag;
		}

		public void setBedrag(BigDecimal bedrag) {
			this.bedrag = bedrag;
		}

		public BigDecimal getBedragReedsBetaald() {
			return bedragReedsBetaald;
		}

		public void setBedragReedsBetaald(BigDecimal bedragReedsBetaald) {
			this.bedragReedsBetaald = bedragReedsBetaald;
		}

		public BigDecimal getOpenstaandBedrag() {
			return openstaandBedrag;
		}

		public void setOpenstaandBedrag(BigDecimal openstaandBedrag) {
			this.openstaandBedrag = openstaandBedrag;
		}
	}

	@Embeddable
	public static class Betaalplan implements Comparable<Betaalplan> {

		private Integer termijnVolgnummer;

		private LocalDate ingangsdatum;

		private BigDecimal bedrag;

		public Betaalplan() {
		}

		public Betaalplan(Integer termijnVolgnummer, LocalDate ingangsdatum, BigDecimal bedrag) {
			this.termijnVolgnummer = termijnVolgnummer;
			this.ingangsdatum = ingangsdatum;
			this.bedrag = bedrag;
		}

		@Override
		public int compareTo(Betaalplan other) {
			return termijnVolgnummer.compareTo(other.termijnVolgnummer);
		}

		public Integer getTermijnVolgnummer() {
			return termijnVolgnummer;
		}

		public void setTermijnVolgnummer(Integer termijnVolgnummer) {
			this.termijnVolgnummer = termijnVolgnummer;
		}

		public LocalDate getIngangsdatum() {
			return ingangsdatum;
		}

		public void setIngangsdatum(LocalDate ingangsdatum) {
			this.ingangsdatum = ingangsdatum;
		}

		public BigDecimal getBedrag() {
			return bedrag;
		}

		public void setBedrag(BigDecimal bedrag) {
			this.bedrag = bedrag;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Betaalplan that = (Betaalplan) o;
			return Objects.equals(termijnVolgnummer, that.termijnVolgnummer) && Objects.equals(ingangsdatum, that.ingangsdatum) && Objects.equals(bedrag, that.bedrag);
		}

		@Override
		public int hashCode() {
			return Objects.hash(termijnVolgnummer, ingangsdatum, bedrag);
		}
	}

}
