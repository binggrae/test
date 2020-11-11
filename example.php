<?php

declare(strict_types=1);

namespace App\Service;

use App\Entity\Address\Area;
use App\Entity\Address\City;
use App\Entity\Address\Region;
use App\Entity\Address\Settlement;
use App\Entity\Address\Street;
use App\Entity\Catalog\Address;
use App\Entity\Catalog\Authority;
use App\Entity\Catalog\Document;
use App\Entity\Catalog\Finance;
use App\Entity\Catalog\Founder;
use App\Entity\Catalog\Inn;
use App\Entity\Catalog\License;
use App\Entity\Catalog\Manager;
use App\Entity\Catalog\Okved;
use App\Entity\Catalog\Organization;
use App\Entity\Catalog\OrganizationAuthority;
use App\Entity\Catalog\ForeignOrganization;
use App\Entity\Catalog\OrganizationFounder;
use App\Entity\Catalog\OrganizationManager;
use App\Entity\Catalog\OrganizationOkved;
use App\Entity\Catalog\Person;
use App\Entity\Catalog\RegistrySmb;
use App\Enum\DocumentTypesEnum;
use App\Enum\FounderTypesEnum;
use App\Enum\ManagerTypesEnum;
use App\Message\Data;
use DateTime;
use Doctrine\ORM\EntityManagerInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Lock\LockFactory;

class DataParserService
{
    private const LOCK_NAME = 'data_parse';
    private const LOCK_TIMEOUT = 10;

    /**
     * @var EntityManagerInterface
     */
    private $entityManager;
    /**
     * @var DataDateParser
     */
    private $dateParser;
    /**
     * @var LoggerInterface
     */
    private $logger;
    /**
     * @var LockFactory
     */
    private $lockFactory;
    /**
     * @var Person[]
     */
    private $persons = [];
    /**
     * @var OrganizationAuthority[]
     */
    private $organizationAuthorities = [];
    /**
     * @var Authority[]
     */
    private $authorities = [];

    public function __construct(
        EntityManagerInterface $entityManager,
        DataDateParser $dateParser,
        LockFactory $lockFactory,
        LoggerInterface $logger
    ) {
        $this->entityManager = $entityManager;
        $this->dateParser = $dateParser;
        $this->lockFactory = $lockFactory;
        $this->logger = $logger;
    }

    public function parse(Data $message)
    {
        $dataContent = $message->getContent();
        $organizationRepository = $this->entityManager->getRepository(Organization::class);
        $organization = $organizationRepository->find($dataContent['data']['inn']);

        if ($organization === null) {
            $this->create($dataContent['data']);;
        } else {
            $this->update($organization, $dataContent['data']);
        }

        $this->clear();

    }

    private function create(array $data): void
    {
        try {
            $this->entityManager->getConnection()->getConfiguration()->setSQLLogger(null);
            $this->entityManager->beginTransaction();

            $organization = $this->fillOrganization(new Organization(), $data);
            $founders = $this->getFounders($data['founders'] ?? [], $organization);
            $managers = $this->getManagers($data['managers'] ?? [], $organization);
            $okveds = $this->getOkveds($data['okveds'] ?? [], $organization);
            $authorities = $this->getAuthorities($data['authorities'] ?? [], $organization);
            $documents = $this->getDocuments($data['documents'] ?? [], $organization);
            $registrySmb = $this->getRegistrySmb($data['documents']['smb'] ?? [], $organization);
            $finance = $this->getFinance($data['finance'] ?? [], $organization);
            $address = $this->getAddress($data['address'] ?? [], $organization);
            $licenses = $this->getLicenses($data['licenses'] ?? [], $organization);

            $this->entityManager->flush();
            $this->entityManager->commit();
            $this->entityManager->clear();
        } catch (\Throwable $exception) {
            $this->entityManager->rollback();
            $this->entityManager->clear();
            $this->logger->error($exception->getMessage());

            throw $exception;
        }

        $a = 0;
    }

    private function update(Organization $organization, array $data): void
    {
        // TODO
    }

    private function fillOrganization(Organization $organization, array $data): Organization
    {
        $organization->setInn($data['inn']);
        $organization->setName($data['name']['full']);
        $organization->setNameShort($data['name']['short']);
        $organization->setNameWithOpf($data['name']['full_with_opf']);
        $organization->setNameShortWithOpf($data['name']['short_with_opf']);
        $organization->setNameLatin($data['name']['latin']);
        $organization->setType($data['type']);
        $organization->setBranchType($data['branch_type']);
        $organization->setBranchCount($data['branch_count']);
        $organization->setOgrn($data['ogrn']);
        $organization->setOgrnDate($this->dateParser->parseTimestampMilliseconds($data['ogrn_date']));
        $organization->setOkpo($data['okpo']);
        $organization->setOkved($data['okved']);
        $organization->setOkvedType($data['okved_type']);
        $organization->setHid($data['hid']);
        $organization->setKpp($data['kpp']);
        $organization->setAddress($data['address']['value']);
        $organization->setAddressUnrestricted($data['address']['unrestricted_value']);
        $organization->setManagementName($data['management']['name']);
        $organization->setManagementPost($data['management']['post']);
        $organization->setCapital((float) $data['capital']['value']);
        $organization->setCapitalType($data['capital']['type']);
        $organization->setStatus($data['state']['status']);
        $organization->setOpf($data['opf']['full']);
        $organization->setOpfShort($data['opf']['short']);
        $organization->setOpfType($data['opf']['type']);
        $organization->setActualityDate(
            $this->dateParser->parseTimestampMilliseconds($data['state']['actuality_date'])
        );
        $organization->setRegistrationDate(
            $this->dateParser->parseTimestampMilliseconds($data['state']['registration_date'])
        );
//        $organization->setLiquidationDate(
//            $this->dateParser->parseTimestampMilliseconds($data['state']['liquidation_date'])
//        );

        if (!empty($data['managers'])) {
            foreach ($data['managers'] as $manager) {
                $managementName = mb_strtolower($manager['fio']['source']);
                $managerName = mb_strtolower($data['management']['name']);

                if (!empty($manager['inn']) && $managementName === $managerName) {
                    $organization->setManagementInn($manager['inn']);
                    break;
                }
            }
        }

        if (!$this->entityManager->isOpen()) {
            $this->entityManager = $this->entityManager->create(
                $this->entityManager->getConnection(),
                $this->entityManager->getConfiguration(),
                $this->entityManager->getEventManager()
            );
        }

        $this->entityManager->persist($organization);

        return $organization;
    }

    private function getFounders(array $rawFounders, Organization $organization): array
    {
        $founders = [];

        foreach ($rawFounders as $rawFounder) {
            $personInn = null;
            $orgInn = null;
            $foreignOrgId = null;
            $founderName = null;

            if ($rawFounder['type'] === FounderTypesEnum::PHYSICAL) {
                $person = $this->getPerson($rawFounder);
                $personInn = $person->getInn();
                $founderName = sprintf(
                    '%s %s %s',
                    $rawFounder['fio']['surname'],
                    $rawFounder['fio']['name'],
                    $rawFounder['fio']['patronymic']
                );
            } elseif ($rawFounder['type'] === FounderTypesEnum::LEGAL) {
                $orgInn = $rawFounder['inn'];
                $founderName = $rawFounder['name'];
            }

            if (empty($personInn) && empty($orgInn)) {
                $foreignOrganization = $this->getForeignOrganization($rawFounder['hid'], $founderName);
                $foreignOrgId = $foreignOrganization ? $foreignOrganization->getId() : null;
            }

            $founderRepository = $this->entityManager->getRepository(Founder::class);
            $founder = $founderRepository->findOneBy([
                'personInn' => $personInn,
                'orgInn' => $orgInn,
                'foreignOrgId' => $foreignOrgId,
            ]);

            if ($founder === null) {
                $founder = new Founder();
                $founder->setPersonInn($personInn);
                $founder->setOrgInn($orgInn);
                $founder->setForeignOrgId($foreignOrgId);
            }

            $organizationFounder = $this->getOrganizationFounder($founder, $organization);

            $organizationFounder->setType($rawFounder['type']);
            $organizationFounder->setShareType($rawFounder['share']['type'] ?? null);
            $organizationFounder->setShareValue($rawFounder['share']['value'] ?? null);
            $organizationFounder->setShareDenominator($rawFounder['share']['denominator'] ?? null);
            $organizationFounder->setShareNumerator($rawFounder['share']['numerator'] ?? null);
            $this->entityManager->persist($organizationFounder);
            $this->entityManager->persist($founder);

            $founders[] = $founder;
        }

        return $founders;
    }

    private function getManagers(array $rawManagers, Organization $organization): array
    {
        $managers = [];

        foreach ($rawManagers as $rawManager) {
            $personInn = null;
            $orgInn = null;
            $foreignOrgId = null;
            $managerName = null;

            if (in_array($rawManager['type'], ManagerTypesEnum::PHYSICAL_TYPES, true) ) {
                $person = $this->getPerson($rawManager);
                $personInn = $person->getInn();
                $managerName = sprintf(
                    '%s %s %s',
                    $rawManager['fio']['surname'],
                    $rawManager['fio']['name'],
                    $rawManager['fio']['patronymic']
                );
            } elseif ($rawManager['type'] === ManagerTypesEnum::LEGAL) {
                $orgInn = $rawManager['inn'];
                $managerName = $rawManager['name'];
            }

            if (empty($personInn) && empty($orgInn)) {
                $foreignOrganization = $this->getForeignOrganization($rawManager['hid'], $managerName);
                $foreignOrgId = $foreignOrganization ? $foreignOrganization->getId() : null;
            }

            $managerRepository = $this->entityManager->getRepository(Manager::class);
            $manager = $managerRepository->findOneBy([
                'personInn' => $personInn,
                'orgInn' => $orgInn,
                'foreignOrgId' => $foreignOrgId,
            ]);

            if ($manager === null) {
                $manager = new Manager();
                $manager->setPersonInn($personInn);
                $manager->setOrgInn($orgInn);
                $manager->setForeignOrgId($foreignOrgId);
            }

            $organizationManager = $this->getOrganizationManager($manager, $organization);

            $organizationManager->setPost($rawManager['post']);
            $organizationManager->setType($rawManager['type']);
            $this->entityManager->persist($organizationManager);

            if ($manager !== null) {
                $this->entityManager->persist($manager);
            }

            $managers[] = $manager;
        }

        return $managers;
    }

    private function getOkveds(array $rawOkveds, Organization $organization): array
    {
        $okveds = [];

        foreach ($rawOkveds as $rawOkved) {
            $okvedRepository = $this->entityManager->getRepository(Okved::class);
            $okved = $okvedRepository->find($rawOkved['code']);

            if ($okved === null) {
                $okved = new Okved();
                $okved->setCode($rawOkved['code']);
                $okved->setName($rawOkved['name']);
                $okved->setType($rawOkved['type']);
            } else {
                $this->entityManager->getUnitOfWork()->registerManaged($okved, [
                    'code' => $okved->getCode(),
                ], []);
                // TODO update
            }

            $organizationOkved = $this->getOrganizationOkved($okved, $organization);
            $organizationOkved->setIsMain($rawOkved['main']);
            $this->entityManager->persist($organizationOkved);
            $this->entityManager->persist($okved);

            $okveds[] = $okved;
        }

        return $okveds;
    }

    private function getAuthorities(array $rawAuthorities, Organization $organization): array
    {
        $authorities = [];

        foreach ($rawAuthorities as $orgType => $rawAuthority) {
            $index = sprintf('%s_%s', $rawAuthority['code'], $rawAuthority['type']);


            $authorityRepository = $this->entityManager->getRepository(Authority::class);
            $authority = $authorityRepository->findOneBy(
                [
                    'code' => $rawAuthority['code'],
                    'type' => $rawAuthority['type'],
                ]
            );


            if ($authority === null) {
                if (isset($this->authorities[$index])) {
                    $authority = $this->authorities[$index];
                } else {
                    $authority = new Authority();
                    $authority->setCode($rawAuthority['code']);
                    $authority->setName($rawAuthority['name']);
                    $authority->setAddress($rawAuthority['address']);
                    $authority->setType($rawAuthority['type']);
                }

                $this->authorities[$index] = $authority;
            } else {
                $this->entityManager->getUnitOfWork()->registerManaged($authority, [
                    'id' => $authority->getId(),
                ], [
                    'id' => $authority->getId(),
                    'code' => $authority->getCode(),
                    'address' => $authority->getAddress(),
                    'type' => $authority->getType(),
                ]);

                // TODO update
            }

            $organizationAuthority = $this->getOrganizationAuthority($authority, $orgType, $organization);
            $this->entityManager->persist($authority);
            $this->entityManager->persist($organizationAuthority);

            $authorities[] = $authority;
        }

        return $authorities;
    }

    private function getDocuments(array $rawDocuments, Organization $organization): array
    {
        $authorities = [];

        foreach ($rawDocuments as $rawDocument) {
            if (!in_array($rawDocument['type'], DocumentTypesEnum::ALL_TYPES, true)) {
                continue;
            }

            $documentRepository = $this->entityManager->getRepository(Document::class);
            $document = $documentRepository->findOneBy([
                'series' => $rawDocument['series'],
                'number' => $rawDocument['number'],
                'organization' => $organization
            ]);

            if ($document === null) {
                $document = new Document();
                $document->setOrganization($organization);
                $document->setType($rawDocument['type']);
                $document->setSeries($rawDocument['series']);
                $document->setNumber($rawDocument['number']);
                $document->setIssueDate($this->dateParser->parseTimestampMilliseconds($rawDocument['issue_date']));
                $document->setIssueAuthority($rawDocument['issue_authority']);
                $this->entityManager->persist($document);
            } else {
                // TODO update
            }

            $authorities[] = $document;
        }

        return $authorities;
    }

    private function getLicenses(array $rawLicenses, Organization $organization): array
    {
        $licenses = [];

        foreach ($rawLicenses as $rawLicens) {
            $licenseRepository = $this->entityManager->getRepository(License::class);
            $license = $licenseRepository->findOneBy([
                'series' => $rawLicens['series'],
                'number' => $rawLicens['number'],
                'organization' => $organization
            ]);

            if ($license === null) {
                $license = new License();
                $license->setOrganization($organization);
                $license->setSeries($rawLicens['series']);
                $license->setNumber($rawLicens['number']);
                $license->setIssueAuthority($rawLicens['issue_authority']);
                $license->setIssueDate($this->dateParser->parseTimestampMilliseconds($rawLicens['issue_date']));
                $license->setSuspendAuthority($rawLicens['suspend_authority']);
                $license->setSuspendDate($this->dateParser->parseTimestampMilliseconds($rawLicens['suspend_date']));
                $license->setValidFrom($this->dateParser->parseTimestampMilliseconds($rawLicens['valid_from']));
                $license->setValidTo($this->dateParser->parseTimestampMilliseconds($rawLicens['valid_to']));
                $this->entityManager->persist($license);
            } else {
                // TODO update
            }

            $licenses[] = $license;
        }

        return $licenses;
    }

    private function getRegistrySmb(array $rawSmb, Organization $organization): ?RegistrySmb
    {
        if ($rawSmb === []) {
            return null;
        }

        $smbRepository = $this->entityManager->getRepository(RegistrySmb::class);
        $smb = $smbRepository->findOneBy([
            'type' => $rawSmb['type'],
            'category' => $rawSmb['category'],
            'organization' => $organization
        ]);

        if ($smb === null) {
            $smb = new RegistrySmb();
            $smb->setOrganization($organization);
            $smb->setType($rawSmb['type']);
            $smb->setCategory($rawSmb['category']);
            $smb->setIssueDate($this->dateParser->parseTimestampMilliseconds($rawSmb['issue_date']));
            $this->entityManager->persist($smb);
        } else {
            // TODO update
        }

        return $smb;
    }

    private function getFinance(array $rawFinance, Organization $organization): ?Finance
    {
        if ($rawFinance === []) {
            return null;
        }

        $financeRepository = $this->entityManager->getRepository(Finance::class);
        $finance = $financeRepository->findOneBy([
            'income' => $rawFinance['income'],
            'expense' => $rawFinance['expense'],
            'debt' => $rawFinance['debt'],
            'penalty' => $rawFinance['penalty'],
            'organization' => $organization
        ]);

        if ($finance !== null) {
            return $finance;
        }

        $finance = new Finance();
        $finance->setOrganization($organization);
        $finance->setTaxSystem($rawFinance['tax_system']);
        $finance->setIncome($rawFinance['income']);
        $finance->setExpense($rawFinance['expense']);
        $finance->setDebt($rawFinance['debt']);
        $finance->setPenalty($rawFinance['penalty']);
        $this->entityManager->persist($finance);

        return $finance;
    }

    private function getPerson(array $data): Person
    {
        $inn = $data['inn'];
        $personRepository = $this->entityManager->getRepository(Person::class);
        $person = $personRepository->find($data['inn']);

        if (isset($this->persons[$inn])) {
            return $this->persons[$inn];
        }

        if ($person === null) {
            $person = new Person();
            $person->setInn($inn);
            $person->setSurname($data['fio']['surname']);
            $person->setName($data['fio']['name']);
            $person->setPatronymic($data['fio']['patronymic']);
            $person->setGender(mb_strtolower($data['fio']['gender']));
            $person->setSource($data['fio']['source']);

            $this->entityManager->persist($person);
        } else {
            $this->entityManager->getUnitOfWork()->registerManaged($person, [
                'inn' => $inn,
            ], []);
        }

        $this->persons[$inn] = $person;

        return $person;
    }

    private function getAddress(array $rawAddress, Organization $organization): ?Address
    {
        if ($rawAddress === []) {
            return null;
        }

        $addressRepository = $this->entityManager->getRepository(Address::class);
        $address = $addressRepository->findOneBy([
            'fiasId' => $rawAddress['data']['fias_id'],
            'organization' => $organization,
        ]);

        if ($address !== null) {
            return $address;
        }

        $address = new Address();
        $address->setOrganization($organization);
        $address->setFiasId($rawAddress['data']['fias_id']);
        $address->setSource($rawAddress['data']['source']);
        $address->setBlock($rawAddress['data']['block']);
        $address->setBlockType($rawAddress['data']['block_type']);
        $address->setTimezone($rawAddress['data']['timezone']);
        $address->setGeoLat($rawAddress['data']['geo_lat']);
        $address->setGeoLon($rawAddress['data']['geo_lon']);
        $address->setCountry($rawAddress['data']['country']);
        $address->setOkato($rawAddress['data']['okato']);
        $address->setOktmo($rawAddress['data']['oktmo']);
        $address->setMetro($rawAddress['data']['metro']);
        $address->setRegion($this->getRegion($rawAddress['data']));
        $address->setArea($this->getArea($rawAddress['data']));
        $address->setCity($this->getCity($rawAddress['data']));
        $address->setSettlement($this->getSettlement($rawAddress['data']));
        $address->setStreet($this->getStreet($rawAddress['data']));
        $address->setHouse($rawAddress['data']['house']);
        $address->setHouseType($rawAddress['data']['house_type_full']);
        $address->setFlat($rawAddress['data']['flat']);
        $address->setFlatType($rawAddress['data']['flat_type_full']);
        $this->entityManager->persist($address);

        return $address;
    }

    private function getRegion(array $rawAddressData): ?Region
    {
        if (empty($rawAddressData['region_fias_id'])) {
            return null;
        }

        $regionRepository = $this->entityManager->getRepository(Region::class);
        $region = $regionRepository->find($rawAddressData['region_fias_id']);

        if ($region !== null) {
            $this->entityManager->getUnitOfWork()->registerManaged($region, [
                'fiasId' => $region->getFiasId(),
            ], []);

            return $region;
        }

        $region = new Region();
        $region->setFiasId($rawAddressData['region_fias_id']);
        $region->setName($rawAddressData['region']);
        $region->setType($rawAddressData['region_type_full']);
        $this->entityManager->persist($region);

        return $region;
    }

    private function getArea(array $rawAddressData): ?Area
    {
        if (empty($rawAddressData['area_fias_id'])) {
            return null;
        }

        $areaRepository = $this->entityManager->getRepository(Area::class);
        $area = $areaRepository->find($rawAddressData['area_fias_id']);

        if ($area !== null) {
            $this->entityManager->getUnitOfWork()->registerManaged($area, [
                'fiasId' => $area->getFiasId(),
            ], []);

            return $area;
        }

        $area = new Area();
        $area->setFiasId($rawAddressData['area_fias_id']);
        $area->setName($rawAddressData['area']);
        $area->setType($rawAddressData['area_type_full']);
        $this->entityManager->persist($area);

        return $area;
    }

    private function getCity(array $rawAddressData): ?City
    {
        if (empty($rawAddressData['city_fias_id'])) {
            return null;
        }

        $cityRepository = $this->entityManager->getRepository(City::class);
        $city = $cityRepository->find($rawAddressData['city_fias_id']);

        if ($city !== null) {
            $this->entityManager->getUnitOfWork()->registerManaged($city, [
                'fiasId' => $city->getFiasId(),
            ], []);

            return $city;
        }

        $city = new City();
        $city->setFiasId($rawAddressData['city_fias_id']);
        $city->setName($rawAddressData['city']);
        $city->setType($rawAddressData['city_type_full']);
        $this->entityManager->persist($city);

        return $city;
    }

    private function getSettlement(array $rawAddressData): ?Settlement
    {
        if (empty($rawAddressData['settlement_fias_id'])) {
            return null;
        }

        $settlementRepository = $this->entityManager->getRepository(Settlement::class);
        $settlement = $settlementRepository->find($rawAddressData['settlement_fias_id']);

        if ($settlement !== null) {
            $this->entityManager->getUnitOfWork()->registerManaged($settlement, [
                'fiasId' => $settlement->getFiasId(),
            ], []);

            return $settlement;
        }

        $settlement = new Settlement();
        $settlement->setFiasId($rawAddressData['settlement_fias_id']);
        $settlement->setName($rawAddressData['settlement']);
        $settlement->setType($rawAddressData['settlement_type_full']);
        $this->entityManager->persist($settlement);

        return $settlement;
    }

    private function getStreet(array $rawAddressData): ?Street
    {
        if (empty($rawAddressData['street_fias_id'])) {
            return null;
        }

        $streetRepository = $this->entityManager->getRepository(Street::class);
        $street = $streetRepository->find($rawAddressData['street_fias_id']);

        if ($street !== null) {
            $this->entityManager->getUnitOfWork()->registerManaged($street, [
                'fiasId' => $street->getFiasId(),
            ], []);

            return $street;
        }

        $street = new Street();
        $street->setFiasId($rawAddressData['street_fias_id']);
        $street->setName($rawAddressData['street']);
        $street->setType($rawAddressData['street_type_full']);
        $this->entityManager->persist($street);

        return $street;
    }

    private function fillPerson(Person $person, array $data): Person
    {
        $person->setInn($data['inn']);
        $person->setSurname($data['fio']['surname']);
        $person->setName($data['fio']['name']);
        $person->setPatronymic($data['fio']['patronymic']);
        $person->setGender(mb_strtolower($data['fio']['gender']));
        $person->setSource($data['fio']['source']);

        return $person;
    }

    private function getOrganizationFounder(Founder $founder, Organization $organization): OrganizationFounder
    {
        $organizationFounderRepository = $this->entityManager->getRepository(OrganizationFounder::class);
        $organizationFounder = $organizationFounderRepository->findOneBy([
            'organization' => $organization,
            'founder' => $founder,
        ]);

        if ($organizationFounder !== null) {
            return $organizationFounder;
        }

        $organizationFounder = new OrganizationFounder();
        $organizationFounder->setOrganization($organization);
        $organizationFounder->setFounder($founder);

        return $organizationFounder;
    }

    private function getForeignOrganization(string $hid, string $name): ForeignOrganization
    {
        $foreignOrganizationRepository = $this->entityManager->getRepository(ForeignOrganization::class);
        $foreignOrganization = $foreignOrganizationRepository->findOneBy([
            'hid' => $hid,
        ]);

        if ($foreignOrganization !== null) {
            return $foreignOrganization;
        }

        $foreignOrganization = new ForeignOrganization();
        $foreignOrganization->setHid($hid);
        $foreignOrganization->setName($name);
        $this->entityManager->persist($foreignOrganization);

        return $foreignOrganization;
    }

    private function getOrganizationManager(Manager $manager, Organization $organization): OrganizationManager
    {
        $organizationManagerRepository = $this->entityManager->getRepository(OrganizationManager::class);
        $organizationManager = $organizationManagerRepository->findOneBy([
            'organization' => $organization,
            'manager' => $manager,
        ]);

        if ($organizationManager !== null) {
            return $organizationManager;
        }

        $organizationManager = new OrganizationManager();
        $organizationManager->setOrganization($organization);
        $organizationManager->setManager($manager);

        return $organizationManager;
    }

    private function getOrganizationOkved(Okved $okved, Organization $organization): OrganizationOkved
    {
        $organizationOkvedRepository = $this->entityManager->getRepository(OrganizationOkved::class);
        $organizationOkved = $organizationOkvedRepository->findOneBy([
            'organization' => $organization,
            'okved' => $okved,
        ]);

        if ($organizationOkved !== null) {
            return $organizationOkved;
        }

        $organizationOkved = new OrganizationOkved();
        $organizationOkved->setOrganization($organization);
        $organizationOkved->setOkved($okved);

        return $organizationOkved;
    }

    private function getOrganizationAuthority(
        Authority $authority,
        string $type,
        Organization $organization
    ): OrganizationAuthority {

        $index = sprintf('%s_%s_%s', $organization->getInn(), $authority->getCode(), $type);

        if (isset($this->organizationAuthorities[$index])) {
            return $this->organizationAuthorities[$index];
        }

        $organizationAuthorityRepository = $this->entityManager->getRepository(OrganizationAuthority::class);
        $organizationAuthority = $organizationAuthorityRepository->findOneBy([
            'organization' => $organization,
            'authority' => $authority,
            'type' => $type,
        ]);

        if ($organizationAuthority !== null) {
            $this->entityManager->getUnitOfWork()->registerManaged($organizationAuthority, [
                'org_inn' => $organization->getInn(),
                'authority_code' => $authority->getCode(),
                'type' => $type,
            ], []);

            return $organizationAuthority;
        }

        $organizationAuthority = new OrganizationAuthority();
        $organizationAuthority->setOrganization($organization);
        $organizationAuthority->setAuthority($authority);
        $organizationAuthority->setType($type);

        $this->organizationAuthorities[$index] = $organizationAuthority;

        return $organizationAuthority;
    }

    private function updateInnLastParsedDate(string $inn): void
    {
        $innRepository = $this->entityManager->getRepository(Inn::class);
        $innEntity = $innRepository->findOneBy([
            'inn' => $inn,
        ]);

        if ($innEntity !== null) {
            $this->entityManager->getUnitOfWork()->registerManaged($innEntity, [
                'id' => $innEntity->getId(),
            ], [
                'id' => $innEntity->getId(),
                'inn' => $innEntity->getInn(),
                'name' => $innEntity->getName(),
                'lastParsedAt' => $innEntity->getLastParsedAt(),
            ]);

            $innEntity->setLastParsedAt(new DateTime());

            $this->entityManager->persist($innEntity);
        } else {
            $this->logger->critical('Inn not found in table: ' . $inn);
        }

    }

    private function clear(): void
    {
        $this->persons = [];
        $this->organizationAuthorities = [];
        $this->authorities = [];
    }
}
